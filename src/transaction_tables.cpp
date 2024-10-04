//
// Created by zhou822 on 5/23/23.
//

#include "core/transaction_tables.hpp"
#include "core/edge_delta_block_state_protection.hpp"
#include "core/bwgraph.hpp"
#include "core/exceptions.hpp"
#include "core/previous_version_garbage_queue.hpp"
namespace GTX{
    inline bool lazy_update(VertexDeltaHeader* vertex_delta, uint64_t txn_id, uint64_t status){
        return vertex_delta->lazy_update(txn_id,status);
    }
    inline bool lazy_update(BaseEdgeDelta* edge_delta, uint64_t txn_id, uint64_t status){
        return edge_delta->lazy_update(txn_id,status);
    }
    //todo:: change back to void
    void ArrayTransactionTable::lazy_update_block(uintptr_t block_ptr) {
        EdgeDeltaBlockHeader* current_edge_delta_block = bwGraph->get_block_manager().convert<EdgeDeltaBlockHeader>(block_ptr);
        uint64_t current_combined_offset = current_edge_delta_block->get_current_offset();
        //if the offset is overflowing, a consolidation will happen soon, so let me exit and let consolidation do lazy update for me.
        if(current_edge_delta_block->is_overflow_offset(current_combined_offset)){
           // return false;
           return;
        }
        //std::unordered_map<uint64_t,int32_t>lazy_update_records;
        uint32_t current_delta_offset = static_cast<uint32_t>(current_combined_offset&SIZE2MASK);
        BaseEdgeDelta* current_delta = current_edge_delta_block->get_edge_delta(current_delta_offset);
        while(current_delta_offset>0){
            uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
            if(original_ts)[[likely]]{
                if(is_txn_id(original_ts)){
                    uint64_t status =0;
                    if(txn_tables->get_status(original_ts,status)){
                        if(status!=IN_PROGRESS){
                            if(status!=ABORT){
#if CHECKED_PUT_EDGE
                                current_edge_delta_block->update_previous_delta_invalidate_ts(current_delta->toID,current_delta->previous_version_offset,status);
#else
                                current_edge_delta_block->update_previous_delta_invalidate_ts(current_delta->toID,current_delta->previous_offset,status);
#endif

                                if(current_delta->lazy_update(original_ts,status)){
#if LAZY_LOCKING
                                    if(current_delta->is_last_delta){
                                        current_edge_delta_block->release_protection(current_delta->toID);
                                    }
#endif
                                   // record_lazy_update_record(&lazy_update_records,original_ts);
                                   txn_tables->reduce_op_count(original_ts,1);
                                }
                            }
#if EDGE_DELTA_TEST
                            if(current_delta->creation_ts!=status){
                                throw EagerCleanException();
                            }
#endif
                        }
                    }
                }
            }
            current_delta++;
            current_delta_offset-=ENTRY_DELTA_SIZE;
        }
      /*  for(auto it = lazy_update_records.begin();it!=lazy_update_records.end();it++){
            txn_tables->reduce_op_count(it->first,it->second);
        }*/
        //return true;
    }

    int32_t ArrayTransactionTable::lazy_update_block(uintptr_t block_ptr, uint64_t txn_id) {
        EdgeDeltaBlockHeader* current_edge_delta_block = bwGraph->get_block_manager().convert<EdgeDeltaBlockHeader>(block_ptr);
        uint64_t current_combined_offset = current_edge_delta_block->get_current_offset();
        //if the offset is overflowing, a consolidation will happen soon, so let me exit and let consolidation do lazy update for me.
        if(current_edge_delta_block->is_overflow_offset(current_combined_offset)){
            // return false;
            return -1;
        }
        std::unordered_map<uint64_t,int32_t>lazy_update_records;
        uint32_t current_delta_offset = static_cast<uint32_t>(current_combined_offset&SIZE2MASK);
        BaseEdgeDelta* current_delta = current_edge_delta_block->get_edge_delta(current_delta_offset);
        int32_t found =0;
        while(current_delta_offset>0){
            uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
            if(original_ts)[[likely]]{
                if(original_ts==txn_id){
                    found++;
                }
                if(is_txn_id(original_ts)){
                    uint64_t status =0;
                    if(txn_tables->get_status(original_ts,status)){
                        if(status!=IN_PROGRESS){
                            if(status!=ABORT){
#if CHECKED_PUT_EDGE
                                current_edge_delta_block->update_previous_delta_invalidate_ts(current_delta->toID,current_delta->previous_version_offset,status);
#else
                                current_edge_delta_block->update_previous_delta_invalidate_ts(current_delta->toID,current_delta->previous_offset,status);
#endif
                                if(current_delta->lazy_update(original_ts,status)){
#if LAZY_LOCKING
                                    if(current_delta->is_last_delta){
                                        current_edge_delta_block->release_protection(current_delta->toID);
                                    }
#endif
                                    record_lazy_update_record(&lazy_update_records,original_ts);
                                }
                            }
#if EDGE_DELTA_TEST
                            if(current_delta->creation_ts!=status){
                                throw EagerCleanException();
                            }
#endif
                        }
                    }
                }
            }
            current_delta++;
            current_delta_offset-=ENTRY_DELTA_SIZE;
        }
        for(auto it = lazy_update_records.begin();it!=lazy_update_records.end();it++){
            txn_tables->reduce_op_count(it->first,it->second);
        }
        return found;
    }
    void ArrayTransactionTable::eager_clean(uint64_t index) {
        //need to access BwGraph and its block manager
        auto& entry = local_table[index];
#if USING_EAGER_CLEAN
        //for debug
      //  uint32_t number_of_cleaned_edge_blocks = 0;
      //  uint32_t number_of_cleaned_vertex_blocks = 0;
      //  int32_t found =0;
    /*    auto all_equal = true;
        timestamp_t first_compare_ts = local_table[0].status.load();
        for(int i=0; i<Per_Thread_Table_Size;i++){
            if(local_table[i].status.load()!=first_compare_ts){
                all_equal=false;
            }
        }
        auto bool_all_in_progress = true;
        if(all_equal){
            if(first_compare_ts==IN_PROGRESS){

            }else{
                bool_all_in_progress = false;
            }
        }*/
#if TRACK_EXECUTION_TIME
        auto start = std::chrono::high_resolution_clock::now();
#endif
        for(size_t i=0; i<entry.touched_blocks.size();i++){
            //if already cleaned-up, can quickly return
            if(!entry.op_count.load(std::memory_order_acquire)){
#if TRACK_EXECUTION_TIME
                auto stop = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
                bwGraph->local_eager_clean_real_work_time.local()+=duration.count();
#endif
                return;
            }
            auto& touched_block_entry = entry.touched_blocks.at(i);
            auto target_vid_label_pair =decompose_block_id(touched_block_entry.block_id);
            auto& index_entry = bwGraph->get_vertex_index_entry(target_vid_label_pair.first);//get vertex index entry
            //if the entry is already invalid, its edge delta blocks must already be lazy updated and freed.
            //todo: check its correctness
            if (!index_entry.valid.load(std::memory_order_acquire)){
                //throw std::runtime_error("touched block not valid?");
                continue;
            }
            //if it refers to labeled edge block
            if(target_vid_label_pair.second){
#if TRACK_EXECUTION_TIME
                auto edge_clean_start = std::chrono::high_resolution_clock::now();
#endif
                //label block never gets deleted because we can reuse it
                EdgeLabelBlock* target_label_block = bwGraph->get_block_manager().convert<EdgeLabelBlock>(index_entry.edge_label_block_ptr.load(std::memory_order_acquire));
                BwLabelEntry* target_label_entry;
                if(target_label_block->reader_lookup_label(target_vid_label_pair.second,target_label_entry)){
                    //if the block is under overflow or installation states, we know another thread is doing consolidation work so it will lazy update our transaction!
                   if( BlockStateVersionProtectionScheme::reader_access_block(thread_id,touched_block_entry.block_id,target_label_entry,bwGraph->get_block_access_ts_table())){
                       //todo:: we add the safety check here: but it requires we cache this value after validation phase
                     //  if(touched_block_entry.block_version_num==target_label_entry->block_version_number.load()){
                           //if the block still exists
                           if(target_label_entry->block_ptr){

                              // lazy_update_block(target_label_entry->block_ptr);
                               //todo::remove this later
                               lazy_update_block(target_label_entry->block_ptr);
                              // found = lazy_update_block(target_label_entry->block_ptr,entry.txn_id);
                              // number_of_cleaned_edge_blocks++;
                           }
                      // }
                       BlockStateVersionProtectionScheme::release_protection(thread_id,bwGraph->get_block_access_ts_table());
                   }//else under consolidation so someone will clean for us
                   else{

                   }
                }else{
                    //never delete entries, at the worst mark the block ptr as 0. So if I modified this label, the entry must exist
                    throw LabelEntryMissingException();
                }
#if TRACK_EXECUTION_TIME
                auto edge_clean_stop = std::chrono::high_resolution_clock::now();
                auto edge_clean_duration = std::chrono::duration_cast<std::chrono::microseconds>(edge_clean_stop - edge_clean_start);
                bwGraph->local_edge_clean_real_work_time.local()+=edge_clean_duration.count();
#endif
            }else{
#if TRACK_EXECUTION_TIME
                auto vertex_clean_start = std::chrono::high_resolution_clock::now();
#endif
                auto current_vertex_ptr = index_entry.vertex_delta_chain_head_ptr.load(std::memory_order_acquire);
                if(current_vertex_ptr){
                    VertexDeltaHeader* vertex_delta = bwGraph->get_block_manager().convert<VertexDeltaHeader>(current_vertex_ptr);
                    auto current_ts = vertex_delta->get_creation_ts();
                    if(is_txn_id(current_ts)){
                        uint64_t status =0;
                        if(txn_tables->get_status(current_ts,status)){
                            if(status!=IN_PROGRESS){
                                if(vertex_delta->lazy_update(current_ts,status)){
                                    txn_tables->reduce_op_count(current_ts,1);
                                    //todo: throw in garbage queue
                                    uintptr_t previous_ptr = vertex_delta->get_previous_ptr();
                                   if(status!=ABORT){
                                       if(previous_ptr){
                                           auto previous_delta = bwGraph->get_block_manager().convert<VertexDeltaHeader>(previous_ptr);
                                           thread_local_garbage_queue->register_entry(previous_ptr,previous_delta->get_order(),status);
                                       }
                                   }else{
                                       std::cout<<"eager abort 3"<<std::endl;
                                       throw EagerAbortException();//aborted deltas should be eager aborted.
                                   }
                                }
                            }
                        }
                    }
                    //todo::remove this later
                   // number_of_cleaned_vertex_blocks++;
                }
#if TRACK_EXECUTION_TIME
                auto vertex_clean_stop = std::chrono::high_resolution_clock::now();
                auto vertex_clean_duration = std::chrono::duration_cast<std::chrono::microseconds>(vertex_clean_stop-vertex_clean_start );
                bwGraph->local_vertex_clean_real_work_time.local()+=vertex_clean_duration.count();
#endif
            }
        }
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        bwGraph->local_eager_clean_real_work_time.local()+=duration.count();
#endif
        //for debug
        size_t count =0;
    /*    bool all_equal_100 = true;
        bool all_equal_1000 = true;
        bool all_equal_10000 = true;
        bool all_equal_100000= true;
        bool all_equal_1000000 = true;
        bool all_equal_10000000 = true;
        timestamp_t to_compare_ts0 = 0;
        timestamp_t to_compare_ts1 = 0;
        timestamp_t to_compare_ts2 = 0;
        timestamp_t to_compare_ts3 = 0;
        timestamp_t to_compare_ts4 = 0;
        timestamp_t to_compare_ts5 = 0;
        while(entry.op_count.load()){
            count++;
            if(count == 100){
                to_compare_ts0 = local_table[0].status.load();
                for(int i=0; i<Per_Thread_Table_Size;i++){
                    if(local_table[i].status.load()!=to_compare_ts0){
                        all_equal_100=false;
                    }
                }
            }
            else if(count == 1000){
                to_compare_ts1 = local_table[0].status.load();
                for(int i=0; i<Per_Thread_Table_Size;i++){
                    if(local_table[i].status.load()!=to_compare_ts1){
                        all_equal_1000=false;
                    }
                }
            }
            else if(count == 10000){
                to_compare_ts2 = local_table[0].status.load();
                for(int i=0; i<Per_Thread_Table_Size;i++){
                    if(local_table[i].status.load()!=to_compare_ts2){
                        all_equal_10000=false;
                    }
                }
            }else if(count == 100000){
                to_compare_ts3 = local_table[0].status.load();
                for(int i=0; i<Per_Thread_Table_Size;i++){
                    if(local_table[i].status.load()!=to_compare_ts3){
                        all_equal_100000=false;
                    }
                }
            }else if(count == 1000000){
                to_compare_ts4 = local_table[0].status.load();
                for(int i=0; i<Per_Thread_Table_Size;i++){
                    if(local_table[i].status.load()!=to_compare_ts4){
                        all_equal_1000000=false;
                    }
                }
            }else if(count == 10000000){
                to_compare_ts5 = local_table[0].status.load();
                for(int i=0; i<Per_Thread_Table_Size;i++){
                    if(local_table[i].status.load()!=to_compare_ts5){
                        all_equal_10000000=false;
                    }
                }
            }else if(count ==100000000){
                throw EagerCleanException();
            }
        }*/

#endif
        size_t count =0;
        while(entry.op_count.load(std::memory_order_acquire)){
            if(count++==10000000000){
                std::cout<<"index is "<<index<<std::endl;
                std::cout<<"txn id is "<<entry.txn_id<<std::endl;
                //std::cout<<"touched block size is "<<entry.touched_blocks.size()<<std::endl;
                std::cout<<"status is "<<entry.status<<" op count is "<<entry.op_count.load()<<std::endl;
                /*auto& touched_block_entry = entry.touched_blocks.at(0);
                auto target_vid_label_pair =decompose_block_id(touched_block_entry.block_id);
                std::cout<<"vid is "<<target_vid_label_pair.first<<std::endl;
                std::cout<<"label is "<<static_cast<uint32_t>(target_vid_label_pair.second)<<std::endl;*/
              /*  std::cout<<"found is "<<found<<std::endl;
                std::cout<<"number of cleaned edge block is "<<number_of_cleaned_edge_blocks<<std::endl;
                std::cout<<"number of cleaned vertex block is "<<number_of_cleaned_vertex_blocks<<std::endl;
                std::cout<<"index is "<<index<<std::endl;
                std::cout<<"txn id is "<<entry.txn_id<<std::endl;
                std::cout<<"touched block size is "<<entry.touched_blocks.size()<<std::endl;
                std::cout<<"status is "<<entry.status<<" op count is "<<entry.op_count.load()<<std::endl;
                auto& touched_block_entry = entry.touched_blocks.at(0);
                auto target_vid_label_pair =decompose_block_id(touched_block_entry.block_id);
                std::cout<<"vid is "<<target_vid_label_pair.first<<std::endl;
                std::cout<<"label is "<<static_cast<uint32_t>(target_vid_label_pair.second)<<std::endl;
                auto& index_entry = bwGraph->get_vertex_index_entry(target_vid_label_pair.first);
                EdgeLabelBlock* target_label_block = bwGraph->get_block_manager().convert<EdgeLabelBlock>(index_entry.edge_label_block_ptr.load());
                BwLabelEntry* target_label_entry;
                if(target_label_block->reader_lookup_label(target_vid_label_pair.second,target_label_entry)){
                    if( BlockStateVersionProtectionScheme::reader_access_block(thread_id,touched_block_entry.block_id,target_label_entry,bwGraph->get_block_access_ts_table())){
                        //todo:: we add the safety check here: but it requires we cache this value after validation phase
                        //  if(touched_block_entry.block_version_num==target_label_entry->block_version_number.load()){
                        //if the block still exists
                        if(target_label_entry->block_ptr){
                            EdgeDeltaBlockHeader* bad_block = bwGraph->get_block_manager().convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
                            bad_block->print_metadata();
                            uint64_t current_offset = bad_block->get_current_offset();
                            uint32_t current_delta_offset = static_cast<uint32_t>(current_offset&SIZE2MASK);
                            while(current_delta_offset>0){
                                auto current_delta = bad_block->get_edge_delta(current_delta_offset);
                                if(current_delta->creation_ts.load()==entry.txn_id){
                                    std::cout<<"current delta with our txn ID found"<<std::endl;
                                }
                                if(current_delta->creation_ts.load()==entry.status.load()){
                                    std::cout<<"current delta with our ts found"<<std::endl;
                                }
                                current_delta_offset-=ENTRY_DELTA_SIZE;
                            }
                            uint32_t previous_counter = 1;
                            auto previous_block = bwGraph->get_block_manager().convert<EdgeDeltaBlockHeader>(bad_block->get_previous_ptr());
                           // previous_block->print_metadata();
                            uint64_t previous_block_offset = previous_block->get_current_offset();
                            uint32_t previous_delta_offset = static_cast<uint32_t>(previous_block_offset&SIZE2MASK);
                            while(previous_delta_offset>0){
                                auto previous_delta = previous_block->get_edge_delta(previous_delta_offset);
                                if(previous_delta->creation_ts.load()==entry.txn_id){
                                    std::cout<<previous_counter<<" previous delta with our txn ID found"<<std::endl;
                                }
                                if(previous_delta->creation_ts.load()==entry.status.load()){
                                    std::cout<<previous_counter<<" previous delta with our ts found"<<std::endl;
                                }
                                previous_delta_offset-=ENTRY_DELTA_SIZE;
                            }
                            while(previous_block->get_previous_ptr()){
                                previous_counter++;
                                previous_block = bwGraph->get_block_manager().convert<EdgeDeltaBlockHeader>(previous_block->get_previous_ptr());
                               // previous_block->print_metadata();
                                 previous_block_offset = previous_block->get_current_offset();
                                 previous_delta_offset = static_cast<uint32_t>(previous_block_offset&SIZE2MASK);
                                while(previous_delta_offset>0){
                                    auto previous_delta = previous_block->get_edge_delta(previous_delta_offset);
                                    if(previous_delta->creation_ts.load()==entry.txn_id){
                                        std::cout<<previous_counter<<" previous delta with our txn ID found"<<std::endl;
                                    }
                                    if(previous_delta->creation_ts.load()==entry.status.load()){
                                        std::cout<<previous_counter<<" previous delta with our ts found"<<std::endl;
                                    }
                                    previous_delta_offset-=ENTRY_DELTA_SIZE;
                                }
                            }
                        }
                        // }
                        BlockStateVersionProtectionScheme::release_protection(thread_id,bwGraph->get_block_access_ts_table());
                    }
                }*/
                throw EagerCleanException();
            }
        }
        //std::cout<<"eager clean succeeds"<<std::endl;
    }

    void ArrayTransactionTable::eager_clean(uint64_t index, std::unordered_set<uint64_t> &cleaned_blocks) {
        //need to access BwGraph and its block manager
        auto& entry = local_table[index];
#if USING_EAGER_CLEAN
#if TRACK_EXECUTION_TIME
        auto start = std::chrono::high_resolution_clock::now();
#endif
        for(size_t i=0; i<entry.touched_blocks.size();i++){
            //if already cleaned-up, can quickly return
            if(!entry.op_count.load()){
#if TRACK_EXECUTION_TIME
                auto stop = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
                bwGraph->local_eager_clean_real_work_time.local()+=duration.count();
#endif
                return;
            }
            auto& touched_block_entry = entry.touched_blocks.at(i);
            auto emplace_result = cleaned_blocks.emplace(touched_block_entry.block_id);
            //will be true the first time we clean it
            if(!emplace_result.second){
                continue;
            }
            auto target_vid_label_pair =decompose_block_id(touched_block_entry.block_id);
            auto& index_entry = bwGraph->get_vertex_index_entry(target_vid_label_pair.first);//get vertex index entry
            //if the entry is already invalid, its edge delta blocks must already be lazy updated and freed.
            //todo: check its correctness
            if (!index_entry.valid.load()){
                throw std::runtime_error("touched block not valid?");
                //continue;
            }
            //if it refers to labeled edge block
            if(target_vid_label_pair.second){
#if TRACK_EXECUTION_TIME
                auto edge_clean_start = std::chrono::high_resolution_clock::now();
#endif
                //label block never gets deleted because we can reuse it
                EdgeLabelBlock* target_label_block = bwGraph->get_block_manager().convert<EdgeLabelBlock>(index_entry.edge_label_block_ptr.load());
                BwLabelEntry* target_label_entry;
                if(target_label_block->reader_lookup_label(target_vid_label_pair.second,target_label_entry)){
                    //if the block is under overflow or installation states, we know another thread is doing consolidation work so it will lazy update our transaction!
                    if( BlockStateVersionProtectionScheme::reader_access_block(thread_id,touched_block_entry.block_id,target_label_entry,bwGraph->get_block_access_ts_table())){
                        //todo:: we add the safety check here: but it requires we cache this value after validation phase
                        //  if(touched_block_entry.block_version_num==target_label_entry->block_version_number.load()){
                        //if the block still exists
                        if(target_label_entry->block_ptr){

                            // lazy_update_block(target_label_entry->block_ptr);
                            //todo::remove this later
                            lazy_update_block(target_label_entry->block_ptr);
                            // found = lazy_update_block(target_label_entry->block_ptr,entry.txn_id);
                            // number_of_cleaned_edge_blocks++;
                        }
                        // }
                        BlockStateVersionProtectionScheme::release_protection(thread_id,bwGraph->get_block_access_ts_table());
                    }//else under consolidation so someone will clean for us
                    else{

                    }
                }else{
                    //never delete entries, at the worst mark the block ptr as 0. So if I modified this label, the entry must exist
                    throw LabelEntryMissingException();
                }
#if TRACK_EXECUTION_TIME
                auto edge_clean_stop = std::chrono::high_resolution_clock::now();
                auto edge_clean_duration = std::chrono::duration_cast<std::chrono::microseconds>(edge_clean_stop - edge_clean_start);
                bwGraph->local_edge_clean_real_work_time.local()+=edge_clean_duration.count();
#endif
            }else{
#if TRACK_EXECUTION_TIME
                auto vertex_clean_start = std::chrono::high_resolution_clock::now();
#endif
                auto current_vertex_ptr = index_entry.vertex_delta_chain_head_ptr.load();
                if(current_vertex_ptr){
                    VertexDeltaHeader* vertex_delta = bwGraph->get_block_manager().convert<VertexDeltaHeader>(current_vertex_ptr);
                    auto current_ts = vertex_delta->get_creation_ts();
                    if(is_txn_id(current_ts)){
                        uint64_t status =0;
                        if(txn_tables->get_status(current_ts,status)){
                            if(status!=IN_PROGRESS){
                                if(vertex_delta->lazy_update(current_ts,status)){
                                    txn_tables->reduce_op_count(current_ts,1);
                                    //todo: throw in garbage queue
                                    uintptr_t previous_ptr = vertex_delta->get_previous_ptr();
                                    if(status!=ABORT){
                                        if(previous_ptr){
                                            auto previous_delta = bwGraph->get_block_manager().convert<VertexDeltaHeader>(previous_ptr);
                                            thread_local_garbage_queue->register_entry(previous_ptr,previous_delta->get_order(),status);
                                        }
                                    }else{
                                        std::cout<<"eager abort 3"<<std::endl;
                                        throw EagerAbortException();//aborted deltas should be eager aborted.
                                    }
                                }
                            }
                        }
                    }
                    //todo::remove this later
                    // number_of_cleaned_vertex_blocks++;
                }
#if TRACK_EXECUTION_TIME
                auto vertex_clean_stop = std::chrono::high_resolution_clock::now();
                auto vertex_clean_duration = std::chrono::duration_cast<std::chrono::microseconds>(vertex_clean_stop-vertex_clean_start );
                bwGraph->local_vertex_clean_real_work_time.local()+=vertex_clean_duration.count();
#endif
            }
        }
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        bwGraph->local_eager_clean_real_work_time.local()+=duration.count();
#endif
        //for debug
        size_t count =0;
        while(entry.op_count.load()){
            if(count++==10000000000){
                throw EagerCleanException();
            }
        }
#endif
    }
    void ArrayTransactionTable::range_eager_clean(uint64_t index) {
        std::unordered_set<uint64_t> cleaned_blocks;
        for(uint64_t i= index; i<(index+clean_threshold);i++){
            if(local_table[i].op_count.load())
                eager_clean(i,cleaned_blocks);
        }
    }
}