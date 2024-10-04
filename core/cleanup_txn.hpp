//
// Created by zhou822 on 7/7/23.
//

#pragma once
#include "bw_index.hpp"
#include "transaction_tables.hpp"
#include "edge_iterator.hpp"
#include "previous_version_garbage_queue.hpp"
#include "types.hpp"
#include "edge_delta_block_state_protection.hpp"
#include <set>
//we separate cleanup transactions from general transactions
namespace GTX{
    class Cleanup_Transaction{
    public:
        Cleanup_Transaction(BwGraph& input_graph, timestamp_t input_ts, TxnTables& input_txn_table, uint8_t input_thread_id):graph(input_graph),read_timestamp(input_ts),txn_tables(input_txn_table),
        block_manager(graph.get_block_manager()),per_thread_garbage_queue(graph.get_per_thread_garbage_queue()),block_access_ts_table(graph.get_block_access_ts_table()),thread_id(input_thread_id){}
        bool work_on_edge_block(uint64_t block_id, uint64_t block_version);
        void force_to_work_on_edge_block(uint64_t block_id);
        inline void commit(){
            batch_lazy_updates();
        }
        void consolidation(BwLabelEntry *current_label_entry, EdgeDeltaBlockHeader *current_block, uint64_t block_id);
        void force_to_consolidation(BwLabelEntry *current_label_entry, EdgeDeltaBlockHeader *current_block, uint64_t block_id);
    private:
        inline void batch_lazy_updates(){
            for(auto it = lazy_update_records.begin();it!=lazy_update_records.end();it++){
                if(it->second>0){
                    txn_tables.reduce_op_count(it->first,it->second);
                }
            }
            lazy_update_records.clear();
        }
        EdgeDeltaInstallResult allocate_delta(EdgeDeltaBlockHeader* current_block, int32_t data_size){
            //todo:; for debug
            /*    if(data_size>128){
                    throw std::runtime_error("for debug, too large delta");
                }*/
            uint32_t block_size = current_block->get_size();
            uint64_t original_block_offset = current_block->allocate_space_for_new_delta(data_size);
#if EDGE_DELTA_TEST
            uint64_t current_new_block_offset = current_block->get_current_offset();
            if(original_block_offset>=current_new_block_offset){
                throw std::runtime_error("for debug, offset overflow");
            }
#endif
            uint32_t original_data_offset = static_cast<uint32_t>(original_block_offset>>32);
            uint32_t original_delta_offset = static_cast<uint32_t>(original_block_offset&SIZE2MASK);
            uint32_t new_data_offset = original_data_offset+data_size;
            uint32_t new_delta_offset = original_delta_offset+ENTRY_DELTA_SIZE;
            current_data_offset = original_data_offset;//grow from left to right;
            current_delta_offset = new_delta_offset; //grow from right to left
            if((new_delta_offset+new_data_offset)>block_size){
                if(original_delta_offset+original_data_offset<=block_size){
                    return EdgeDeltaInstallResult::CAUSE_OVERFLOW;
                }else{
                    return EdgeDeltaInstallResult::ALREADY_OVERFLOW;
                }
            }
            return EdgeDeltaInstallResult::SUCCESS;
        }
        inline order_t calculate_new_fit_order(uint64_t block_storage_size){
            order_t new_order = size_to_order(block_storage_size);
            uint64_t new_size = 1ul<<new_order;
            //if the next order is less than 25% larger than the current size, double the new size
            if(new_size-block_storage_size<block_storage_size/4){
                return new_order+1;
            }else{
                //otherwise return the next size
                return new_order;
            }
        }
        BwGraph& graph;
        const timestamp_t read_timestamp;
        TxnTables& txn_tables;
        lazy_update_map lazy_update_records;
        BlockManager& block_manager;
        GarbageBlockQueue& per_thread_garbage_queue;
        BlockAccessTimestampTable& block_access_ts_table;
        uint8_t thread_id;
        uint32_t current_delta_offset;
        uint32_t current_data_offset;
    };
}//namespace bwgraph

