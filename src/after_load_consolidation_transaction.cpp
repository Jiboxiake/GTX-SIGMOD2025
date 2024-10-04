//
// Created by zhou822 on 7/31/23.
//
#include "core/after_load_consolidation_transaction.hpp"

using namespace GTX;

void AfterLoadCleanupTransaction::consolidate_edge_delta_block(GTX::vertex_t vid, GTX::label_t label) {
    auto& vertex_index_entry = graph.get_vertex_index_entry(vid);
    if(!vertex_index_entry.valid.load())[[unlikely]]{
        return;
    }
    BwLabelEntry* target_label_entry;
    auto edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
    auto found = edge_label_block->reader_lookup_label(label,target_label_entry);
    if(!found){
        return;
    }
    auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
    auto current_combined_offset = current_block->get_current_offset();
    uint32_t current_delta_offset = EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(current_combined_offset);
    auto current_delta = current_block->get_edge_delta(current_delta_offset);
    bool need_consolidation = false;
    while(current_delta_offset){
        if(current_delta->creation_ts.load(std::memory_order_acquire)==ABORT)[[unlikely]]{
            need_consolidation = true;
            break;
        }
        current_delta++;
        current_delta_offset-=ENTRY_DELTA_SIZE;
    }
    if(need_consolidation)[[unlikely]]{
        current_delta_offset = EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(current_combined_offset);
        current_delta = current_block->get_edge_delta(current_delta_offset);
        uint64_t new_block_size = 64;
        std::vector<uint32_t>latest_versions;
        latest_versions.reserve(current_delta_offset/ENTRY_DELTA_SIZE-1);
        uint64_t largest_invalidation_ts = 0;
        while(current_delta_offset){
            if(current_delta->creation_ts.load(std::memory_order_acquire)!=ABORT)[[likely]]{
                latest_versions.emplace_back(current_delta_offset);
                new_block_size+= ENTRY_DELTA_SIZE+current_delta->data_length;
                largest_invalidation_ts = std::max(current_delta->invalidate_ts.load(std::memory_order_acquire),largest_invalidation_ts);
            }
            current_delta++;
            current_delta_offset-=ENTRY_DELTA_SIZE;
        }
        auto new_order = size_to_order(new_block_size);
        auto new_block_ptr = block_manager.alloc(new_order);
        auto new_block = block_manager.convert<EdgeDeltaBlockHeader>(new_block_ptr);
        new_block->fill_metadata(vid,largest_invalidation_ts,graph.get_commit_manager().get_current_read_ts(),0,new_order, &graph.get_txn_tables(),target_label_entry->delta_chain_index);
        target_label_entry->delta_chain_index->clear();
        target_label_entry->delta_chain_index->resize(new_block->get_delta_chain_num());
        for(int64_t i = static_cast<int64_t>(latest_versions.size()-1); i>=0; i--){
            current_delta = current_block->get_edge_delta(latest_versions.at(i));
            auto append_result = new_block->checked_append_edge_delta(current_delta->toID,current_delta->creation_ts.load(std::memory_order_acquire),
                                                                      current_delta->delta_type,current_block->get_edge_data(current_delta->data_offset),
                                                                      current_delta->data_length,target_label_entry->delta_chain_index->at(new_block->get_delta_chain_id(current_delta->toID)).get_offset(),0);
            if(append_result.first!=EdgeDeltaInstallResult::SUCCESS)[[unlikely]]{
                throw ConsolidationException();
            }
            target_label_entry->delta_chain_index->at(new_block->get_delta_chain_id(current_delta->toID)).update_offset(append_result.second);
        }
        uint8_t * ptr = block_manager.convert<uint8_t>(target_label_entry->block_ptr);
        auto original_order = current_block->get_order();
        memset(ptr,'\0',1ul<<original_order);//zero out memory I used
        block_manager.free(target_label_entry->block_ptr,original_order);
        target_label_entry->block_version_number.fetch_add(1,std::memory_order_acq_rel);
        target_label_entry->block_ptr = new_block_ptr;
    }
}