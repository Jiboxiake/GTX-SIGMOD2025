//
// Created by zhou822 on 5/28/23.
//
#pragma once
//#ifndef BWGRAPH_V2_PREVIOUS_VERSION_GARBAGE_QUEUE_HPP
//#define BWGRAPH_V2_PREVIOUS_VERSION_GARBAGE_QUEUE_HPP
#include "types.hpp"
#include "block_manager.hpp"
#include <queue>
namespace GTX{
    struct PreviousVersionBlockEntry{
        PreviousVersionBlockEntry(){}
        PreviousVersionBlockEntry( const PreviousVersionBlockEntry& other){
            block_ptr = other.block_ptr;
            order = other.order;
            updated_ts = other.updated_ts;
        }
        PreviousVersionBlockEntry& operator = (const PreviousVersionBlockEntry& other){
            block_ptr = other.block_ptr;
            order = other.order;
            updated_ts = other.updated_ts;
            return *this;
        }
        PreviousVersionBlockEntry(uintptr_t input_ptr,order_t input_order, uint64_t input_ts):block_ptr(input_ptr), order(input_order),updated_ts(input_ts){}
        bool operator < (const PreviousVersionBlockEntry& other)const{
            return updated_ts>other.updated_ts;
        }
        bool operator ==(const PreviousVersionBlockEntry& other)const{
            return updated_ts == other.updated_ts;
        }
        uintptr_t block_ptr;
        order_t order;
        uint64_t updated_ts;//the timestamp the new version was created
    };

#if USING_QUEUE_PREVIOUS_VERSION
    class GarbageBlockQueue{
    public:
        GarbageBlockQueue(BlockManager* input_manager):block_manager(input_manager){}
        inline void free_block(uint64_t safe_ts){
            while(!previous_versions_queue.empty()){
                if(previous_versions_queue.front().updated_ts<=safe_ts){
                    //std::cout<<safe_ts<<std::endl;
                    auto& current_top_entry = previous_versions_queue.front();
                    uint8_t * ptr = block_manager->convert<uint8_t>(current_top_entry.block_ptr);
                    memset(ptr,'\0',1ul<<current_top_entry.order);//zero out memory I used
                    block_manager->free(current_top_entry.block_ptr,current_top_entry.order);
                    total_garbage_size-= 1ul<<current_top_entry.order;
                    previous_versions_queue.pop();
                }else{
                    return;
                }
            }
        }
        inline void register_entry(uintptr_t block_ptr, order_t order, uint64_t updated_ts){
#if TRACK_GARBAGE_RECORD_TIME
            auto start = std::chrono::high_resolution_clock::now();
#endif
            total_garbage_size+= 1ul<<order;
            previous_versions_queue.emplace(block_ptr,order,updated_ts);
#if TRACK_GARBAGE_RECORD_TIME
            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
            block_manager->record_garbage_record_time(duration.count());
#endif
        }
        inline bool need_collection(){
            return total_garbage_size>=garbage_collection_size_threshold;
        }
        inline std::queue<PreviousVersionBlockEntry>& get_queue(){return previous_versions_queue;}
        inline void print_status(){
            std::cout<<"remaining garbage size is "<<total_garbage_size<<" bytes"<<" remaining entry number is "<< previous_versions_queue.size()<<std::endl;
        }
        inline bool has_entries(){
            return !previous_versions_queue.empty();
        }
    private:
        //std::priority_queue<PreviousVersionBlockEntry> previous_versions_queue;
        std::queue<PreviousVersionBlockEntry> previous_versions_queue;
        uint64_t total_garbage_size =0;
        BlockManager* block_manager;

    };
#else
    class GarbageBlockQueue{
    public:
        GarbageBlockQueue(BlockManager* input_manager):block_manager(input_manager){}
        inline void free_block(uint64_t safe_ts){
            while(!previous_versions_queue.empty()){
                if(previous_versions_queue.top().updated_ts<=safe_ts){
                    //std::cout<<safe_ts<<std::endl;
                    auto current_top_entry = previous_versions_queue.top();
                    uint8_t * ptr = block_manager->convert<uint8_t>(current_top_entry.block_ptr);
                    memset(ptr,'\0',1ul<<current_top_entry.order);//zero out memory I used
                    block_manager->free(current_top_entry.block_ptr,current_top_entry.order);
                    total_garbage_size-= 1ul<<current_top_entry.order;
                    previous_versions_queue.pop();
                }else{
                    return;
                }
            }
        }
        inline void register_entry(uintptr_t block_ptr, order_t order, uint64_t updated_ts){
            total_garbage_size+= 1ul<<order;
            previous_versions_queue.emplace(block_ptr,order,updated_ts);
        }
        inline bool need_collection(){
            return total_garbage_size>=garbage_collection_size_threshold;
        }
        inline std::priority_queue<PreviousVersionBlockEntry>& get_queue(){return previous_versions_queue;}
        inline void print_status(){
            std::cout<<"remaining garbage size is "<<total_garbage_size<<" bytes"<<" remaining entry number is "<< previous_versions_queue.size()<<std::endl;
        }
        inline bool has_entries(){
            return !previous_versions_queue.empty();
        }
    private:
        std::priority_queue<PreviousVersionBlockEntry> previous_versions_queue;
        //std::queue<PreviousVersionBlockEntry> previous_versions_queue;
        uint64_t total_garbage_size =0;
        BlockManager* block_manager;
    };
#endif

}
//#endif //BWGRAPH_V2_PREVIOUS_VERSION_GARBAGE_QUEUE_HPP
