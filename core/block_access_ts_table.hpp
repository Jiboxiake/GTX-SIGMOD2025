//
// Created by zhou822 on 5/28/23.
//
#pragma once
//#ifndef BWGRAPH_V2_BLOCK_ACCESS_TS_TABLE_HPP
//#define BWGRAPH_V2_BLOCK_ACCESS_TS_TABLE_HPP
#include "exceptions.hpp"
#include <atomic>
#include "graph_global.hpp"
#include "types.hpp"
namespace GTX{
    struct alignas(64) BlockAccessTSEntry{
        BlockAccessTSEntry():accessed_block_id(0),current_ts(0){}
        BlockAccessTSEntry(const BlockAccessTSEntry& other):accessed_block_id(other.accessed_block_id.load()), current_ts(other.current_ts.load()){}
        std::atomic_uint64_t accessed_block_id=0;
        std::atomic_uint64_t current_ts=0;
        char padding[48];
    };
    struct alignas(64) BlockAccessEntry{
        BlockAccessEntry():accessed_block_id(0){}
        BlockAccessEntry(const BlockAccessEntry& other){
            accessed_block_id.store(other.accessed_block_id.load(std::memory_order_acquire),std::memory_order_release);
        }
        std::atomic_uint64_t accessed_block_id=0;
        char padding[56];
    };
    struct alignas(64) TSEntry{
        TSEntry():current_ts(0){}
        TSEntry(const TSEntry& other){
            current_ts.store(other.current_ts.load(std::memory_order_acquire),std::memory_order_release);
        }
        std::atomic_uint64_t current_ts=0;
        char padding[56];
    };
    static_assert(sizeof(BlockAccessTSEntry)==64);
    class BlockAccessTimestampTable{
    public:
#if USING_SEPARATED_TABLES
        inline void store_block_access(uint8_t thread_id, uint64_t block_id){block_access_table[thread_id].accessed_block_id.store(block_id);}
        inline void release_block_access(uint8_t thread_id){block_access_table[thread_id].accessed_block_id.store(BAD_BLOCK_ID);}
        inline void set_total_worker_thread_num(uint64_t worker_num){
            block_access_table.clear();
            ts_table.clear();
            for(uint64_t i=0; i<worker_num; i++){
                ts_table.emplace_back();
            }
            for(uint64_t i=0; i<worker_num;i++){
                block_access_table.emplace_back();
            }
        }
        inline void set_total_worker_thread_num(uint64_t bwgraph_workers, uint64_t openmp_workers){
            block_access_table.clear();
            ts_table.clear();
            for(uint64_t i=0; i<bwgraph_workers+1; i++){
                ts_table.emplace_back();
            }
            for(uint64_t i=0; i<bwgraph_workers+openmp_workers;i++){
                block_access_table.emplace_back();
            }
        }
        bool is_safe(uint8_t thread_id, uint64_t block_id){
            for(uint8_t i=0; i<static_cast<uint8_t>(block_access_table.size());i++){
                if(i==thread_id){
                    //todo:: optimize this part
                    if(block_access_table[i].accessed_block_id.load(/*std::memory_order_acquire*/)!=block_id){
                        throw BlockSafeAccessException();
                    }
                    continue;
                }
                else if(block_access_table[i].accessed_block_id.load(/*std::memory_order_acquire*/)==block_id){
                    return false;
                }
            }
            return true;
        }
        inline void store_current_ts(uint8_t thread_id, timestamp_t read_ts){ts_table[thread_id].current_ts.store(read_ts/*,std::memory_order_release*/);}
        inline uint64_t get_total_thread_num(){return ts_table.size();}
        /*
         * if a read_ts is x, it can read deltas created at x or >x, and it cannot read deltas invalidated at x, so all deltas invalidated at ts <= 10 is considered safe to deallocate
         * blocks invalidated at time <= safe_ts can be deallocated.
         */
        uint64_t calculate_safe_ts(){
            uint64_t min_ts = std::numeric_limits<uint64_t>::max();
            for(uint64_t i=0; i<ts_table.size();i++){
                uint64_t current_ts = ts_table[i].current_ts.load(/*std::memory_order_acquire*/);
                min_ts = (current_ts<min_ts)?current_ts:min_ts;
            }
            return min_ts;
        }
        inline void thread_exit(uint8_t thread_id){
            ts_table[thread_id].current_ts.store(std::numeric_limits<uint64_t>::max()/*,std::memory_order_release*/);
        }
        void print_ts_status(){
            for(size_t i=0; i<ts_table.size();i++){
                std::cout<<"worker thread "<<i<<" has last ts as "<<ts_table[i].current_ts<<std::endl;
            }
        }
#else
        //inline void store_block_access(uint8_t thread_id, uint64_t block_id){table[thread_id].accessed_block_id.store(block_id,std::memory_order_release);}
        inline void store_block_access(uint8_t thread_id, uint64_t block_id){table[thread_id].accessed_block_id.store(block_id);}
        inline void release_block_access(uint8_t thread_id){table[thread_id].accessed_block_id.store(BAD_BLOCK_ID);}
        inline void set_total_worker_thread_num(uint64_t num){
            table.clear();
            table.reserve(num);
            for(uint64_t i=0; i<num;i++){
                table.emplace_back();
            }
        }
        bool is_safe(uint8_t thread_id, uint64_t block_id){
            for(uint8_t i=0; i<static_cast<uint8_t>(table.size()); i++){
                if(i==thread_id){
                    //todo:: optimize this part
                    if(table[i].accessed_block_id.load(/*std::memory_order_acquire*/)!=block_id){
                        throw BlockSafeAccessException();
                    }
                    continue;
                }
                else if(table[i].accessed_block_id.load(/*std::memory_order_acquire*/)==block_id){
                    return false;
                }
            }
            return true;
        }
        //thread does not reset this value at txn finish, just overwrite when new txn is created
        inline void store_current_ts(uint8_t thread_id, timestamp_t read_ts){table[thread_id].current_ts.store(read_ts/*,std::memory_order_release*/);}

        inline uint64_t get_total_thread_num(){return table.size();}
        /*
         * if a read_ts is x, it can read deltas created at x or >x, and it cannot read deltas invalidated at x, so all deltas invalidated at ts <= 10 is considered safe to deallocate
         * blocks invalidated at time <= safe_ts can be deallocated.
         */
        uint64_t calculate_safe_ts(){
            uint64_t min_ts = std::numeric_limits<uint64_t>::max();
            for(uint64_t i=0; i<table.size();i++){
                uint64_t current_ts = table[i].current_ts.load(/*std::memory_order_acquire*/);
                min_ts = (current_ts<min_ts)?current_ts:min_ts;
            }
            return min_ts;
        }
        inline void thread_exit(uint8_t thread_id){
            table[thread_id].current_ts.store(std::numeric_limits<uint64_t>::max()/*,std::memory_order_release*/);
        }
        void print_ts_status(){
            for(size_t i=0; i<table.size();i++){
                std::cout<<"worker thread "<<i<<" has last ts as "<<table[i].current_ts<<std::endl;
            }
        }
#endif
    private:
#if USING_SEPARATED_TABLES
        std::vector<BlockAccessEntry> block_access_table;
        std::vector<TSEntry> ts_table;
#else
        //std::array<BlockAccessTSEntry,worker_thread_num> table;
        std::vector<BlockAccessTSEntry> table;
#endif
    };
}
//#endif //BWGRAPH_V2_BLOCK_ACCESS_TS_TABLE_HPP