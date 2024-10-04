//
// Created by zhou822 on 5/28/23.
//
#pragma once
//#ifndef BWGRAPH_V2_COMMIT_MANAGER_HPP
//#define BWGRAPH_V2_COMMIT_MANAGER_HPP
#include "spin_latch.hpp"
#include <queue>
#include "graph_global.hpp"
#include <array>
#include "transaction_tables.hpp"
namespace GTX{
    //using CommitQueue = std::queue<entry_ptr>;
    struct CommitQueue{
        inline void emplace(entry_ptr txn_entry){
            queue_buffer.emplace(txn_entry);
        }
        std::queue<entry_ptr> queue_buffer;
        std::array<bool,worker_thread_num> already_committed;
    };

    //todo: examine no server thread solution
    class LatchDoubleBufferCommitManager{
    public:
#if USING_ARRAY_TABLE
        LatchDoubleBufferCommitManager(/*ArrayTransactionTables& graph_txn_table*/):latch()/*,txn_tables(graph_txn_table)*/{
            for(int i=0; i<2; i++){
                for(uint32_t j=0; j<worker_thread_num;j++){
                    double_buffer_queue[i].already_committed[j]=false;
                }
            }
        }
#else
        CommitManager(ConcurrentTransactionTables& graph_txn_table):latch(),txn_tables(graph_txn_table){}
#endif
        inline uint64_t get_current_read_ts(){return global_read_epoch.load();}
        inline void txn_commit(uint8_t thread_id,entry_ptr txn_entry){
            while(true){
                latch.lock();
                if(double_buffer_queue[offset].already_committed[thread_id]){
                    //do collaborative commit
                    collaborative_commit();
                    continue;
                }
                double_buffer_queue[offset].emplace(txn_entry);
                double_buffer_queue[offset].already_committed[thread_id]=true;
                latch.unlock();
                return;
            }
        }
        //with latch held
        void collaborative_commit();
        void server_loop();//server loops to commit
        inline void shutdown_signal(){
            running.store(false,std::memory_order_release);
        }

    private:
        std::atomic_bool running = true;
        std::atomic_uint64_t global_read_epoch = 0;
        uint64_t global_write_epoch = 0;
       // void batch_commit();
        uint8_t offset = 0 ;
        spinlock latch;
        spinlock commit_latch;
        std::array<CommitQueue,2>double_buffer_queue;
#if USING_ARRAY_TABLE
       // ArrayTransactionTables& txn_tables;
#else
        ConcurrentTransactionTables& txn_tables;
#endif
       // size_t current_entries = 0;
    };
    struct alignas(64) padded_txn_entry_ptr{
        padded_txn_entry_ptr(){
            txn_ptr.store(nullptr,std::memory_order_release);
            for(int i=0; i<56;i++){
                padding[i]=0;
            }
        }
        padded_txn_entry_ptr(const padded_txn_entry_ptr& other){
            txn_ptr.store(other.txn_ptr.load(std::memory_order_acquire),std::memory_order_release);
            for(int i=0; i<56;i++){
                padding[i]=0;
            }
        }
        std::atomic<entry_ptr> txn_ptr;
        char padding[56];
    };
    class ConcurrentArrayCommitManager{
    public:
        ConcurrentArrayCommitManager(){
            commit_array.resize(current_writer_num);
            for(uint32_t i=0; i<current_writer_num;i++){
                commit_array[i].txn_ptr.store(nullptr,std::memory_order_release);
            }
        }
        //void resize(uint64_t new_txn_num=)
        /*
         * the current implementation still assumes lazy update, in eager commit model, we can do different
         */
        inline bool txn_commit(uint8_t thread_id,entry_ptr txn_entry, [[maybe_unused]] bool willing_to_wait){
#if USING_EAGER_COMMIT
            //Libin's July 25th debug
            auto current_txn_ptr = commit_array[thread_id].txn_ptr.load();
            if(current_txn_ptr&&current_txn_ptr->status.load()==0)[[unlikely]]{
                std::cout<<current_txn_ptr->txn_id<<" "<<current_txn_ptr->status<<std::endl;
                throw std::runtime_error("how is this possible?");
            }
            commit_array[thread_id].txn_ptr.store(txn_entry,std::memory_order_release);
            return true;
            entry_ptr null_ptr = nullptr;
            if(willing_to_wait){
                //todo:: check memory order more
                while(!commit_array[thread_id].txn_ptr.compare_exchange_strong(null_ptr, txn_entry, std::memory_order_acq_rel)){
                    null_ptr = nullptr;
                    //do something?
                }
                return true;
            }else{
                return commit_array[thread_id].txn_ptr.compare_exchange_strong(null_ptr, txn_entry, std::memory_order_acq_rel);
            }
#else
            entry_ptr null_ptr = nullptr;
            if(willing_to_wait){
                //todo:: check memory order more
                while(!commit_array[thread_id].txn_ptr.compare_exchange_strong(null_ptr, txn_entry, std::memory_order_acq_rel)){
                    null_ptr = nullptr;
                    //do something?
                }
                return true;
            }else{
                return commit_array[thread_id].txn_ptr.compare_exchange_strong(null_ptr, txn_entry, std::memory_order_acq_rel);
            }
#endif
        }
        inline void resize_commit_array(uint64_t new_writer_size){
            current_writer_num = new_writer_size;
            commit_array.resize(current_writer_num);
            for(uint32_t i=0; i<current_writer_num;i++){
                commit_array[i].txn_ptr.store(nullptr,std::memory_order_release);
            }
        }
        void server_loop();

#if ENSURE_DURABILITY
        void wal_server_loop();
        void mmap_wal_server_loop();
        void asynchronous_wal_loop();
        void simplified_asynchronous_wal_loop();
        const static size_t buffer_size = 4096;
        struct log_buffer{
            uint32_t buffer_index=0;
            uint32_t buffer_offset=0;
            char buffers[3][buffer_size];
        };
        void write_to_buffer(log_buffer& buffers, std::string& data,  std::ofstream& log_file);
#endif
        inline uint64_t get_current_read_ts(){return global_read_epoch.load(std::memory_order_acquire);}
        inline void shutdown_signal(){running.store(false,std::memory_order_release);}
        inline void restart(){running.store(true,std::memory_order_release);}
    private:
        //std::array<padded_txn_entry_ptr, max_writer_num> commit_array;
        std::vector<padded_txn_entry_ptr> commit_array;
        uint64_t current_writer_num = worker_thread_num;
        //std::vector<padded_txn_entry_ptr> commit_array;
        std::atomic_bool running = true;
        std::atomic_uint64_t global_read_epoch = 0;
        uint64_t global_write_epoch = 0;
        uint32_t offset = 0 ;
    };
    using CommitManager = ConcurrentArrayCommitManager;
}
//#endif //BWGRAPH_V2_COMMIT_MANAGER_HPP
