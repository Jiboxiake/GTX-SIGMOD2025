//
// Created by zhou822 on 5/28/23.
//

#include "core/bwgraph.hpp"
#include "core/gtx_transaction.hpp"
#include "core/cleanup_txn.hpp"
#include <omp.h>
using namespace GTX;

/*BwGraph::~BwGraph(){
    auto max_vid = vertex_index.get_current_allocated_vid();
    for(vertex_t vid = 1; vid<=max_vid; vid++){
        auto& vertex_index_entry = vertex_index.get_vertex_index_entry(vid);
        auto label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
        label_block->deallocate_all_delta_chains_indices();
    }
}*/

ROTransaction BwGraph::begin_read_only_transaction() {
    auto read_ts = commit_manager.get_current_read_ts();
    uint8_t worker_thread_id = thread_manager.get_worker_thread_id();
    block_access_ts_table.store_current_ts(worker_thread_id,read_ts);
    //if(/*garbage_queues[worker_thread_id].need_collection()||*/executed_txn_count.local()==garbage_collection_transaction_threshold||garbage_queues[worker_thread_id].get_queue().size()>=garbage_collection_entry_num_threshold){
    executed_txn_count.local()++;
    if(/*garbage_queues[worker_thread_id].need_collection()||*/executed_txn_count.local()%garbage_collection_transaction_threshold==0/*||garbage_queues[worker_thread_id].get_queue().size()>=garbage_collection_entry_num_threshold*/){
        if(garbage_queues.at(worker_thread_id).has_entries()){
            auto safe_ts = block_access_ts_table.calculate_safe_ts();
            garbage_queues[worker_thread_id].free_block(safe_ts);
        }
        //executed_txn_count.local()=1;
    }/*else{
        executed_txn_count.local()++;
    }*/
    return ROTransaction(*this,read_ts,txn_tables,block_manager,garbage_queues[worker_thread_id],block_access_ts_table,worker_thread_id);

}

RWTransaction BwGraph::begin_read_write_transaction() {
    //eager clean work:
#if USING_EAGER_CONSOLIDATION
    if(thread_local_update_count.local()>eager_blocks_clean_threshold){
        eager_consolidation_clean();
    }
#endif //USING_EAGER_CONSOLIDATION
#if TRACK_EXECUTION_TIME
    auto start = std::chrono::high_resolution_clock::now();
    auto read_ts = commit_manager.get_current_read_ts();
    uint8_t worker_thread_id = thread_manager.get_worker_thread_id();
    auto stop_thread_id = std::chrono::high_resolution_clock::now();
#if USING_RANGE_CLEAN
    auto txn_id = txn_tables.get_table(worker_thread_id).periodic_clean_generate_txn_id();
#else
    auto txn_id = txn_tables.get_table(worker_thread_id).generate_txn_id();
#endif
    auto stop_txn_id = std::chrono::high_resolution_clock::now();
    auto txn_entry =  txn_tables.get_table(worker_thread_id).put_entry(txn_id);
    auto stop_txn_entry= std::chrono::high_resolution_clock::now();
    block_access_ts_table.store_current_ts(worker_thread_id,read_ts);
    if(/*garbage_queues[worker_thread_id].need_collection()||*/executed_txn_count.local()==garbage_collection_transaction_threshold/*||garbage_queues[worker_thread_id].get_queue().size()>=garbage_collection_entry_num_threshold*/){
        auto safe_ts = block_access_ts_table.calculate_safe_ts();
        garbage_queues[worker_thread_id].free_block(safe_ts);
        executed_txn_count.local()=1;
    }else{
        executed_txn_count.local()++;
    }
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    local_rwtxn_creation_time.local()+= duration.count();
    auto get_thread_id_time = std::chrono::duration_cast<std::chrono::microseconds>(stop_thread_id - start);
    local_get_thread_id_time.local()+=get_thread_id_time.count();
    auto generate_txn_id_time = std::chrono::duration_cast<std::chrono::microseconds>(stop_txn_id-stop_thread_id);
    local_generate_txn_id_time.local()+=generate_txn_id_time.count();
    auto install_txn_entry_time = std::chrono::duration_cast<std::chrono::microseconds>(stop_txn_entry-stop_txn_id);
    local_install_txn_entry_time.local()+=install_txn_entry_time.count();
    auto garbage_collection_time = std::chrono::duration_cast<std::chrono::microseconds>(stop - stop_txn_entry);
    local_garbage_collection_time.local()+=garbage_collection_time.count();
     return RWTransaction(*this,txn_id,read_ts,txn_entry,txn_tables,commit_manager,block_manager,garbage_queues[worker_thread_id],block_access_ts_table,recycled_vids[worker_thread_id]);
#else //TRACK_EXECUTION_TIME
    auto read_ts = commit_manager.get_current_read_ts();
    uint8_t worker_thread_id = thread_manager.get_worker_thread_id();
#if USING_RANGE_CLEAN
    auto txn_id = txn_tables.get_table(worker_thread_id).periodic_clean_generate_txn_id();
#else
    auto txn_id = txn_tables.get_table(worker_thread_id).generate_txn_id();
#endif
    auto txn_entry =  txn_tables.get_table(worker_thread_id).put_entry(txn_id);
    block_access_ts_table.store_current_ts(worker_thread_id,read_ts);
//#if !USING_COMMIT_WAIT_WORK
    executed_txn_count.local()++;
#if ENSURE_DURABILITY
    if(executed_txn_count.local()%(1ul<<16)==0){
        do_checkpointing();
    }
#endif
    if(/*garbage_queues[worker_thread_id].need_collection()||*/executed_txn_count.local()%garbage_collection_transaction_threshold==0/*||garbage_queues[worker_thread_id].get_queue().size()>=garbage_collection_entry_num_threshold*/){
        if(garbage_queues.at(worker_thread_id).has_entries()){
            auto safe_ts = block_access_ts_table.calculate_safe_ts();
            garbage_queues[worker_thread_id].free_block(safe_ts);
        }
#if ENSURE_DURABILITY
        //do checkpointing
#endif
        //executed_txn_count.local()=1;
    }/*else{
        executed_txn_count.local()++;
    }*/
//#endif //USING_COMMIT_WAIT_WORK
    return RWTransaction(*this,txn_id,read_ts,txn_entry,txn_tables,commit_manager,block_manager,garbage_queues[worker_thread_id],block_access_ts_table,recycled_vids[worker_thread_id]);
 /*   auto read_ts = commit_manager.get_current_read_ts();
    uint8_t worker_thread_id = thread_manager.get_worker_thread_id();
    auto txn_id = txn_tables.get_table(worker_thread_id).generate_txn_id();
    auto txn_entry =  txn_tables.get_table(worker_thread_id).put_entry(txn_id);
    block_access_ts_table.store_current_ts(worker_thread_id,read_ts);
    if(executed_txn_count.local()==garbage_collection_threshold||garbage_queues[worker_thread_id].get_queue().size()>=garbage_collection_threshold){
        auto safe_ts = block_access_ts_table.calculate_safe_ts();
        garbage_queues[worker_thread_id].free_block(safe_ts);
        executed_txn_count.local()=1;
    }else{
        executed_txn_count.local()++;
    }
    return RWTransaction(*this,txn_id,read_ts,txn_entry,txn_tables,commit_manager,block_manager,garbage_queues[worker_thread_id],block_access_ts_table,recycled_vids[worker_thread_id]);*/
#endif //TRACK_EXECUTION_TIME
}

SharedROTransaction BwGraph::begin_shared_ro_transaction() {
    auto read_ts = commit_manager.get_current_read_ts();
   // uint8_t worker_thread_id = thread_manager.get_openmp_worker_thread_id();
   // block_access_ts_table.store_current_ts(worker_thread_id,read_ts);//the creator thread is in charge of storing the transaction ts in the table
   // if(/*garbage_queues[worker_thread_id].need_collection()||*/executed_txn_count.local()==garbage_collection_transaction_threshold||garbage_queues[worker_thread_id].get_queue().size()>=garbage_collection_entry_num_threshold){
   //     auto safe_ts = block_access_ts_table.calculate_safe_ts();
   //     garbage_queues[worker_thread_id].free_block(safe_ts);
   //     executed_txn_count.local()=1;
   // }else{
   //     executed_txn_count.local()++;
   // }
  // std::cout<<"read ts is "<<read_ts<<std::endl;
   on_openmp_transaction_start(read_ts);
  // print_ts_table();
   //print_garbage_status();
   return SharedROTransaction(*this, read_ts, txn_tables, block_manager, block_access_ts_table);
}

void BwGraph::execute_manual_delta_block_checking(GTX::vertex_t vid) {
    auto& vertex_index_entry = get_vertex_index_entry(vid);
    auto label_block = get_block_manager().convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
    BwLabelEntry* current_label_entry;
    for(label_t label =1; label<=3; label++){
        size_t total_committed_delta_count_from_secondary_index = 0;
        size_t total_committed_delta_count_from_pure_scan = 0;
        if(!label_block->reader_lookup_label(label,current_label_entry)){
            continue;
        }
        auto current_block = get_block_manager().convert<EdgeDeltaBlockHeader>(current_label_entry->block_ptr);
        auto delta_chains_num = current_block->get_delta_chain_num();
        if(delta_chains_num!= static_cast<int32_t>(current_label_entry->delta_chain_index->size())){
            throw std::runtime_error("delta chain num mismatch");
        }
        std::unordered_map<vertex_t , timestamp_t>track_invalidate_ts;
        struct pair_hash{
            std::size_t operator()(std::pair<vertex_t , uint32_t>const & v)const{
                return std::hash<int64_t>()(v.first)+std::hash<uint32_t>()(v.second);
            }
        };
        std::unordered_set<std::pair<vertex_t ,uint32_t>,pair_hash>secondary_index_committed_entries;
        for(size_t i=0; i<current_label_entry->delta_chain_index->size();i++){
            uint32_t offset = current_label_entry->delta_chain_index->at(i).get_offset();
            if(offset&LOCK_MASK){
                throw std::runtime_error("locks should be unlocked already");
            }
            while(offset){
                BaseEdgeDelta* delta = current_block->get_edge_delta(offset);
                if(is_txn_id(delta->creation_ts.load())){
                    throw LazyUpdateException();
                }
                if(delta->creation_ts==ABORT){
                    throw LazyUpdateException();
                }
                if(static_cast<delta_chain_id_t>(delta->toID)%delta_chains_num!=static_cast<delta_chain_id_t>(i)){
                    throw DeltaChainCorruptionException();
                }
                total_committed_delta_count_from_secondary_index++;
                /* char* data = current_block->get_edge_data(delta->data_offset);
                 char to_compare = static_cast<char>(delta->toID%32);
                 for(uint32_t j=0; j<delta->data_length;j++){
                     if(data[j]!=to_compare){
                         throw TransactionReadException();
                     }
                 }*/
                if(!secondary_index_committed_entries.emplace(std::pair<int64_t,uint32_t>(delta->toID,offset)).second){
                    throw new std::runtime_error("error, duplicate entry");
                }
                offset = delta->previous_offset;
            }
        }
        size_t total_size =0;
        uint64_t current_offsets = current_block->get_current_offset();
        uint64_t original_data_offset = current_offsets>>32;
        uint64_t original_delta_offset = current_offsets&SIZE2MASK;
        uint32_t current_delta_offset = static_cast<uint32_t>(current_offsets&SIZE2MASK);
        BaseEdgeDelta* current_delta = current_block->get_edge_delta(current_delta_offset);
        while(current_delta_offset){
            //total_size+=current_delta->data_length+ENTRY_DELTA_SIZE;
            total_size+=ENTRY_DELTA_SIZE;
            if(current_delta->data_length>16){
                total_size+=current_delta->data_length;
            }
            if(is_txn_id((current_delta->creation_ts.load()))){
                throw LazyUpdateException();
            }else if(current_delta->creation_ts!=ABORT){
                if(!secondary_index_committed_entries.count(std::pair<vertex_t, uint32_t>(current_delta->toID,current_delta_offset))){
                    throw std::runtime_error("found an entry not captured by the delta chains");
                }
                if(track_invalidate_ts.count(current_delta->toID)){
                    if(current_delta->invalidate_ts!=track_invalidate_ts.at(current_delta->toID)){
                        std::cout<<current_delta->invalidate_ts<<" "<<track_invalidate_ts.at(current_delta->toID)<<std::endl;
                        //throw std::runtime_error("invalidation ts mismatch");
                    }
                    track_invalidate_ts.insert_or_assign(current_delta->toID, current_delta->creation_ts.load());
                }else{
                    if(current_delta->invalidate_ts!=0){
                        throw std::runtime_error("invalidation ts mismatch");
                    }
                    if(!track_invalidate_ts.try_emplace(current_delta->toID,current_delta->creation_ts.load()).second){
                        throw std::runtime_error("should succeed");
                    }
                }
                total_committed_delta_count_from_pure_scan++;
            }
            /*  char* data = current_block->get_edge_data(current_delta->data_offset);
              char to_compare = static_cast<char>(current_delta->toID%32);
              for(uint32_t j=0; j<current_delta->data_length;j++){
                  if(data[j]!=to_compare){
                      throw TransactionReadException();
                  }
              }*/
            current_delta++;
            current_delta_offset-=ENTRY_DELTA_SIZE;
        }
        if(original_data_offset+original_delta_offset!=total_size){
            throw new std::runtime_error("error, the offset did not correctly represent the delta allocation");
        }
        if(total_committed_delta_count_from_pure_scan!=total_committed_delta_count_from_secondary_index){
            throw new std::runtime_error("error, secondary index does not contain all committed deltas");
        }
    }
}

void BwGraph::eager_consolidation_clean() {
    auto read_ts = commit_manager.get_current_read_ts();
    uint8_t worker_thread_id = thread_manager.get_worker_thread_id();
    block_access_ts_table.store_current_ts(worker_thread_id,read_ts);
    //start a cleanup transaction
    auto cleanup_txn = Cleanup_Transaction(*this,read_ts,txn_tables,worker_thread_id);
    for(auto it = to_check_blocks.local().begin(); it!= to_check_blocks.local().end();){
        auto result = cleanup_txn.work_on_edge_block(it->first,it->second);
        if(result){
            it = to_check_blocks.local().erase(it);
        }else{
            it++;
        }
    }
    cleanup_txn.commit();
    thread_local_update_count.local()=0;
}
void BwGraph::eager_consolidation_on_edge_delta_block(GTX::vertex_t vid, GTX::label_t label) {
    auto& vertex_index_entry = get_vertex_index_entry(vid);
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
        new_block->fill_metadata(vid,largest_invalidation_ts,commit_manager.get_current_read_ts(),0,new_order, &txn_tables,target_label_entry->delta_chain_index);
        target_label_entry->delta_chain_index->clear();
        target_label_entry->delta_chain_index->resize(new_block->get_delta_chain_num());
        for(int64_t i = static_cast<int64_t>(latest_versions.size()-1); i>=0; i--){
            current_delta = current_block->get_edge_delta(latest_versions.at(i));
            char* data;
            if(current_delta->data_length<=16){
                data = current_delta->data;
            }else{
                data = current_block->get_edge_data(current_delta->data_offset);
            }
            auto append_result = new_block->checked_append_edge_delta(current_delta->toID,current_delta->creation_ts.load(std::memory_order_acquire),
                                                                      current_delta->delta_type,data,
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
void BwGraph::force_consolidation_clean() {
    auto read_ts = commit_manager.get_current_read_ts();
    uint8_t worker_thread_id = thread_manager.get_worker_thread_id();
    block_access_ts_table.store_current_ts(worker_thread_id,read_ts);
    //start a cleanup transaction
    auto cleanup_txn = Cleanup_Transaction(*this,read_ts,txn_tables,worker_thread_id);
    for(auto it = to_check_blocks.local().begin(); it!= to_check_blocks.local().end();it++){
        cleanup_txn.force_to_work_on_edge_block(it->first);
    }
    to_check_blocks.local().clear();
    thread_local_update_count.local()=0;
    cleanup_txn.commit();
}

#if ENSURE_DURABILITY

void BwGraph::do_checkpointing() {
    checkpoint.start_checkpoint();
}
void BwGraph::checkpoint_vertex(uint8_t worker_thread_id,GTX::vertex_t vid, uint64_t last_watermark_ts,std::string& checkpoint_data) {
    auto read_ts = commit_manager.get_current_read_ts();
    block_access_ts_table.store_current_ts(worker_thread_id,read_ts);
    SharedROTransaction txn(*this, read_ts, txn_tables, block_manager, block_access_ts_table);
    auto vertex_delta = txn.get_vertex_delta(vid,worker_thread_id);
    std::unordered_set<vertex_t> seen_edges;
    if(vertex_delta){
        //checkpoint_data.append(std::to_string(vid).append(","));
        checkpoint_data.append("v");//mark a new vertex begins
        checkpoint_data.append(std::string_view((char*)&vid,sizeof (vid)));
       if(vertex_delta->get_creation_ts()>=last_watermark_ts){
           checkpoint_data.append("Y");//has vertex data
           auto v_ts = vertex_delta->get_creation_ts();
           checkpoint_data.append(std::string_view((char*)&v_ts,sizeof(v_ts)));
           //checkpoint_data.append(std::to_string(vertex_delta->get_creation_ts()).append(","));
           //checkpoint_data.append(std::to_string(*reinterpret_cast<uint64_t*>(vertex_delta->get_data())));
           checkpoint_data.append(std::string_view(vertex_delta->get_data(),vertex_delta->get_data_size()));
       }else{
           checkpoint_data.append("N");//no vertex data
       }
       //checkpoint_data.append((","));
       uint32_t current_delta_offset = 0;
       //todo: do checkpointing of all labels
       auto current_block = txn.get_block_header(vid,1,worker_thread_id,&current_delta_offset);
       //checkpoint_data.append(std::to_string(1).append(",").append(std::to_string(current_block->get_order())).append(","));
       label_t label =1;
       checkpoint_data.append(std::string_view ((char*)&label,sizeof (label)));
       order_t order = current_block->get_order();
       checkpoint_data.append(std::string_view ((char*)&order,sizeof (order)));
       BaseEdgeDelta* current_delta = current_block->get_edge_delta(current_delta_offset);
        while(current_delta_offset>0){
            //need lazy update
            auto original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
            if(!original_ts)[[unlikely]]{
                current_delta_offset -= ENTRY_DELTA_SIZE;
                current_delta++;
                continue;
            }
            if(is_txn_id(original_ts))[[unlikely]]{
                uint64_t status = 0;
                if (txn_tables.get_status(original_ts, status))[[likely]] {
                    if (status == IN_PROGRESS)[[likely]] {
                        current_delta_offset -= ENTRY_DELTA_SIZE;
                        current_delta++;
                        continue;
                    } else {
                        if (status != ABORT)[[likely]] {
#if CHECKED_PUT_EDGE
                            current_block->update_previous_delta_invalidate_ts(current_delta->toID,
                                                                               current_delta->previous_version_offset,
                                                                               status);
#else
                            current_delta_block->update_previous_delta_invalidate_ts(current_delta->toID,current_delta->previous_offset,status);
#endif
                            if (current_delta->lazy_update(original_ts, status)) {
#if LAZY_LOCKING
                                if(current_delta->is_last_delta.load(std::memory_order_acquire)){
                                            current_delta_block-> release_protection(current_delta->toID);
                                        }
#endif
                                //record lazy update
                                txn_tables.reduce_op_count(original_ts,1);
                            }
                        }
#if EDGE_DELTA_TEST
                        if(current_delta->creation_ts.load(std::memory_order_acquire)!=status){
                                                throw LazyUpdateException();
                                            }
#endif
                        original_ts = status;
                    }
                } else {
                    original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                }
            }
            /*if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                        std::memory_order_acquire);
                //found the visible edge
                if(original_ts<=read_ts&&(current_invalidation_ts==0||current_invalidation_ts>read_ts)){
                    incoming_total += outgoing_contrib[current_delta->toID-1];
                }
            }*/
            //the latest non-abort not in progress delta, and passes last watermark
            if(original_ts>=last_watermark_ts&&!is_txn_id(original_ts)&&original_ts!=ABORT){
                auto emplace_result = seen_edges.emplace(current_delta->toID);
                if(emplace_result.second){
                    //checkpoint_data.append(std::to_string(current_delta->toID).append(","));
                    checkpoint_data.append("e");//mark a new edge begins.
                    auto to_id = current_delta->toID;
                    checkpoint_data.append(std::string_view ((char*)&to_id,sizeof (to_id)));
                    auto edge_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                    checkpoint_data.append(std::string_view ((char*)&edge_ts,sizeof (edge_ts)));
                    //checkpoint_data.append(std::to_string(current_delta->creation_ts.load(std::memory_order_acquire)).append(","));
                    if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                        //checkpoint_data.append(std::to_string(current_delta->delta_type).append(","));
                        /*if(current_delta->delta_type==EdgeDeltaType::UPDATE_DELTA){
                            checkpoint_data.append("U");
                        }else{
                            checkpoint_data.append("I");
                        }*/
                        checkpoint_data.append("U");
                        //checkpoint_data.append(std::to_string(current_delta->data_length).append(","));
                        checkpoint_data.append(std::string_view (current_delta->get_data(),current_delta->data_length));
                        //checkpoint_data.append(std::to_string(*reinterpret_cast<double*>(current_delta->data)));
                        //checkpoint_data.append(";");
                    }else{
                        checkpoint_data.append("D");
                    }
                }
            }
#if USING_READER_PREFETCH
            //if(current_delta_offset>=prefetch_offset)
                                _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
            current_delta_offset -= ENTRY_DELTA_SIZE;
            current_delta++;
        }
        txn.unregister_thread_block_access(worker_thread_id);
        //checkpoint_data.append("|");
    }
    //start getting the deltas
    /*
    auto& vertex_index_entry = get_vertex_index_entry(vid);
    if(vertex_index_entry.valid.load(std::memory_order_acquire)){
        auto vertex_delta_offset = vertex_index_entry.vertex_delta_chain_head_ptr.load(std::memory_order_acquire);
        if(vertex_delta_offset)[[likely]]{
           auto current_vertex_delta = block_manager.convert<VertexDeltaHeader>(vertex_delta_offset);
           auto original_ts = current_vertex_delta->get_creation_ts();
           if(is_txn_id(original_ts))[[unlikely]]{
               //do a lazy update
               uint64_t status;
               if(txn_tables.get_status(original_ts,status)){
                   if(status==IN_PROGRESS){
                       //do nothing
                   }else if(status!=ABORT){
                       if(current_vertex_delta->lazy_update(original_ts,status)){
                           if(current_vertex_delta->get_previous_ptr()){
                               auto previous_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta->get_previous_ptr());
                               garbage_queues[worker_thread_id].register_entry(current_vertex_delta->get_previous_ptr(),previous_vertex_delta->get_order(),status);
#if !USING_COMMIT_WAIT_WORK
                               if(per_thread_garbage_queue.need_collection()){
                                    auto safe_ts = block_access_ts_table.calculate_safe_ts();
                                    per_thread_garbage_queue.free_block(safe_ts);
                                }
#endif
                           }
                       }
#if TXN_TEST
                       if(current_vertex_delta->get_creation_ts()!=status){
                            throw VertexDeltaException();
                        }
#endif
                       original_ts=status;
                   }else{
                       original_ts=status;
                   }
               }else{
                   original_ts = current_vertex_delta->get_creation_ts();
               }
           }
           //if()
        }
    }
    */
}

/*
 * function to bulk load the checkpoint file to build the graph with ts.
 */
//todo: change the function to: check ts, then install if ts is larger.
uint64_t BwGraph::process_vertex_checkpoint(uint64_t current_offset, std::vector<char> &buffer) {
    size_t size = buffer.size();
    if(current_offset<size){
        assert(buffer[current_offset]=='v');
        current_offset++;
        uint64_t vid =  *reinterpret_cast<uint64_t*>(buffer.data()+current_offset);
        //auto vertex_emplace_result = graph_checkpoint.emplace()
        //std::cout<<"vertex id is "<<vid<<std::endl;
        current_offset+=8;
        //assert(buffer[current_offset]=='Y'||buffer[current_offset]=='N');
        if(buffer[current_offset]=='Y'){
            current_offset++;
            uint64_t vertex_ts = *reinterpret_cast<uint64_t*>(buffer.data()+current_offset);
            //std::cout<<"vertex ts is "<<vertex_ts<<std::endl;
            current_offset+=8;
            uint64_t external_vid = *reinterpret_cast<uint64_t*>(buffer.data()+current_offset);
            //std::cout<<"external vid is "<<external_vid<<std::endl;
            current_offset+=8;
            auto& vertex_entry = vertex_index.get_vertex_index_entry(vid);
            order_t new_order = size_to_order(sizeof(VertexDeltaHeader)+8);
            auto new_delta_ptr = block_manager.alloc(new_order);
            auto new_vertex_delta = block_manager.convert<VertexDeltaHeader>(new_delta_ptr);
            new_vertex_delta->fill_metadata(vertex_ts,8,new_order,0);
            new_vertex_delta->write_data((const char*)(&external_vid));
            vertex_entry.vertex_delta_chain_head_ptr.store(new_delta_ptr,std::memory_order_release);
        }else if(buffer[current_offset]=='N'){
            current_offset++;
        }else{
            assert(false);
        }

        uint16_t label =*reinterpret_cast<uint16_t*>(buffer.data()+current_offset);
        //std::cout<<"label is "<<label<<std::endl;
        current_offset+=2;
        uint8_t order = *reinterpret_cast<uint8_t*>(buffer.data()+current_offset);
        current_offset+=1;
        // std::cout<<"order is "<<(int)order<<std::endl;
        //assert(buffer[current_offset]=='e');
        //process all edges
        auto* edge_label_entry = allocate_label_edge_deltas_block(vid,label,order);
        auto* current_block = block_manager.convert<EdgeDeltaBlockHeader>(edge_label_entry->block_ptr);
        uint64_t installed_edge_deltas_num=0;
        while(buffer[current_offset]=='e'){
            current_offset++;
            uint64_t dst = *reinterpret_cast<uint64_t*>(buffer.data()+current_offset);
            //std::cout<<"edge dst is "<<dst<<std::endl;
            current_offset+=8;
            uint64_t edge_ts = *reinterpret_cast<uint64_t*>(buffer.data()+current_offset);
            //std::cout<<"edge ts is "<<edge_ts<<std::endl;
            current_offset+=8;
            if(buffer[current_offset]=='U'){
                current_offset++;
                double edge_weight = *reinterpret_cast<double*>(buffer.data()+current_offset);
                //std::cout<<"edge weight is "<<edge_weight<<std::endl;
                current_offset+=8;
                auto delta_chain_id = current_block->get_delta_chain_id(dst);
                char* data = (char*)(&edge_weight);
                auto& delta_chains_index_entry = edge_label_entry->delta_chain_index->at(delta_chain_id);
                uint32_t new_block_delta_offset = delta_chains_index_entry.get_offset();
                auto append_result = current_block->checked_append_edge_delta(dst,edge_ts,EdgeDeltaType::INSERT_DELTA,data,8,new_block_delta_offset,0);
                if(append_result.first!=EdgeDeltaInstallResult::SUCCESS||!append_result.second)[[unlikely]]{
                    throw std::runtime_error("error, install insert edge delta");
                }
                //std::cout<<"loading edge "<<v<<" "<<edge.dst<<std::endl;
                delta_chains_index_entry.update_offset(append_result.second);
            }else if(buffer[current_offset]=='D'){
                //std::cout<<"edge deletion"<<std::endl;
                //do nothing
                current_offset++;
                auto delta_chain_id = current_block->get_delta_chain_id(dst);
                char* data = nullptr;
                auto& delta_chains_index_entry = edge_label_entry->delta_chain_index->at(delta_chain_id);
                uint32_t new_block_delta_offset = delta_chains_index_entry.get_offset();
                auto append_result = current_block->checked_append_edge_delta(dst,edge_ts,EdgeDeltaType::DELETE_DELTA,data,0,new_block_delta_offset,0);
                if(append_result.first!=EdgeDeltaInstallResult::SUCCESS||!append_result.second)[[unlikely]]{
                    throw std::runtime_error("error, install delete edge delta");
                }
                //std::cout<<"loading edge "<<v<<" "<<edge.dst<<std::endl;
                delta_chains_index_entry.update_offset(append_result.second);
            }else{
                assert(false);
            }
            installed_edge_deltas_num++;
        }
    }
    return current_offset;
}

void BwGraph::process_checkpoint_file(std::string &path) {
    //atomic operation on index, then process the file
    std::ifstream file(path, std::ios::binary | std::ios::ate);
    std::streamsize size = file.tellg();
    file.seekg(0, std::ios::beg);
    std::vector<char> buffer(size);
    uint64_t current_offset =0;
    if (file.read(buffer.data(), size))
    {
        while(current_offset<size){
            current_offset = process_vertex_checkpoint(current_offset,buffer);
        }
    }
    file.close();
}

void BwGraph::load_checkpoint() {
    std::vector<uint64_t> checkpoint_timestamps;
    omp_set_num_threads(4);
    std::string checkpoint_root_directory_path = "/scratch1/zhou822/GTX_checkpoint/merged_checkpoint/";
    for (const auto & entry : std::filesystem::directory_iterator(checkpoint_root_directory_path)){
        //std::cout << entry.path() << std::endl;
        uint64_t index = 0;
        for(uint64_t i = entry.path().string().size()-1; i>=0; i--){
            if(entry.path().string().c_str()[i]=='/'){
                index = i;
                break;
            }
        }
        uint64_t timestamp = std::stoul(entry.path().string().substr(index+1));
        //std::cout<<timestamp<<std::endl;
        checkpoint_timestamps.emplace_back(timestamp);
    }
    std::sort(checkpoint_timestamps.begin(),checkpoint_timestamps.end(),std::greater<>());
    bool is_first = true;
    uint64_t max_vid = 0;
    timestamp_t  max_checkpoint_ts = *checkpoint_timestamps.begin();
    for(auto checkpoint_ts : checkpoint_timestamps){
        auto checkpoint_directory_path = checkpoint_root_directory_path;
        checkpoint_directory_path.append(std::to_string(checkpoint_ts));
        checkpoint_directory_path.append("/");
        //uint64_t max_file_no=0;
        std::vector<std::string>checkpoint_files;
        for (const auto & entry : std::filesystem::directory_iterator(checkpoint_directory_path)){
            if(entry.path().string().find("max_vid.data")!=std::string::npos){
                continue;
            }
            checkpoint_files.emplace_back(entry.path());
        }
        if(is_first){
            //load max_vid and modify the graph
            auto max_vid_path = checkpoint_root_directory_path;
            max_vid_path.append(std::to_string(checkpoint_ts));
            max_vid_path.append("/max_vid.data");
            std::string line;
            std::ifstream max_vid_file(max_vid_path);
            if(std::getline(max_vid_file,line)){
                max_vid = std::stoul(line);
                allocate_max_vid(max_vid);
            }else{
                throw std::runtime_error("error, missing max vid information");
            }
            is_first = false;
        }
#pragma omp parallel for
        for(uint64_t i=0; i< checkpoint_files.size(); i++){
            auto& path = checkpoint_files[i];
            process_checkpoint_file(path);
        }
        //load all checkpoint file names under a vector, then process them in parallel
        /*std::vector<std::thread>workers;
        workers.reserve(worker_thread_num);
        for(uint64_t i=0; i<worker_thread_num;i++){
            //workers.emplace_back(&CheckpointMerger::process_checkpoint_file,this,i,checkpoint_ts);
        }
        for(uint64_t i=0; i<worker_thread_num;i++){
            workers[i].join();
        }*/

        std::cout<<"checkpoint "<<checkpoint_ts<<" finished"<<std::endl;
        //break;//todo: comment this out
    }
    std::cout<<"checkpoint reloaded finished, start log replay"<<std::endl;
    replay_log(max_checkpoint_ts);
}

/*
 * our recovery ensures operations of the same vertex is done by a single thread
 */
void BwGraph::redo_log_operation(GTX::BwGraph::log_operation &op) {
    auto& vertex_entry = vertex_index.get_vertex_index_entry(op.src);//todo:: ideally we should also consider the case vertex entry doesn't exist
    if(op.type==WALType::VERTEX_UPDATE){
        auto v_ptr = vertex_entry.vertex_delta_chain_head_ptr.load(std::memory_order_acquire);
        if(v_ptr){
            auto original_vertex_delta = block_manager.convert<VertexDeltaHeader>(v_ptr);
            if(original_vertex_delta->get_creation_ts()<op.timestamp){
                order_t new_order = size_to_order(sizeof(VertexDeltaHeader)+8);
                auto new_delta_ptr = block_manager.alloc(new_order);
                auto new_vertex_delta = block_manager.convert<VertexDeltaHeader>(new_delta_ptr);
                new_vertex_delta->fill_metadata(op.timestamp,8,new_order,v_ptr);
                new_vertex_delta->write_data((const char*)(&op.v_data));
                vertex_entry.vertex_delta_chain_head_ptr.store(new_delta_ptr,std::memory_order_release);
            }
        }else{
            order_t new_order = size_to_order(sizeof(VertexDeltaHeader)+8);
            auto new_delta_ptr = block_manager.alloc(new_order);
            auto new_vertex_delta = block_manager.convert<VertexDeltaHeader>(new_delta_ptr);
            new_vertex_delta->fill_metadata(op.timestamp,8,new_order,0);
            new_vertex_delta->write_data((const char*)(&op.v_data));
            vertex_entry.vertex_delta_chain_head_ptr.store(new_delta_ptr,std::memory_order_release);
        }
    }else if(op.type==WALType::VERTEX_DELETE){
        //we assume vertex deletion log also includes which edges it deletes
        //so just put vertex deletion delta here
        auto v_ptr = vertex_entry.vertex_delta_chain_head_ptr.load(std::memory_order_acquire);
        if(v_ptr){
            auto original_vertex_delta = block_manager.convert<VertexDeltaHeader>(v_ptr);
            if(original_vertex_delta->get_creation_ts()<op.timestamp){
                order_t new_order = size_to_order(sizeof(VertexDeltaHeader)+8);
                auto new_delta_ptr = block_manager.alloc(new_order);
                auto new_vertex_delta = block_manager.convert<VertexDeltaHeader>(new_delta_ptr);
                new_vertex_delta->fill_metadata(op.timestamp,8,new_order,v_ptr);
                new_vertex_delta->write_data((const char*)(&op.v_data));
                vertex_entry.vertex_delta_chain_head_ptr.store(new_delta_ptr,std::memory_order_release);
            }
        }else{
            order_t new_order = size_to_order(sizeof(VertexDeltaHeader)+8);
            auto new_delta_ptr = block_manager.alloc(new_order);
            auto new_vertex_delta = block_manager.convert<VertexDeltaHeader>(new_delta_ptr);
            new_vertex_delta->fill_metadata(op.timestamp,8,new_order,0);
            new_vertex_delta->write_data((const char*)(&op.v_data));
            vertex_entry.vertex_delta_chain_head_ptr.store(new_delta_ptr,std::memory_order_release);
        }
    }else{
        if(!vertex_entry.edge_label_block_ptr){
            vertex_entry.edge_label_block_ptr = block_manager.alloc(size_to_order(sizeof(EdgeLabelBlock)));
            auto edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_entry.edge_label_block_ptr);
            edge_label_block->fill_information(op.src,&block_manager);
        }
        auto edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_entry.edge_label_block_ptr);
        //either access an existing entry or creating a new entry
        auto* label_entry =  edge_label_block->writer_lookup_label(op.label,&txn_tables,op.timestamp);
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(label_entry->block_ptr);
        //check previous version first
        auto delta_chain_id = current_block->get_delta_chain_id(op.dst);
        auto current_delta_chain_head_offset = label_entry->delta_chain_index->at(delta_chain_id).get_offset();
        //auto previous_version_offset = current_block->fetch_previous_version_offset_simple(op.dst,current_delta_chain_head_offset,op.timestamp);
        uint32_t previous_delta_offset = 0;
        auto previous_delta = current_block->log_redo_find_previous_delta(op.dst,current_delta_chain_head_offset,&previous_delta_offset);
        if(previous_delta){
            assert(previous_delta_offset>0);
            if(previous_delta->creation_ts>=op.timestamp){
                return;
            }
        }
        uint32_t block_size = current_block->get_size();
        auto original_block_offset = current_block->allocate_space_for_new_delta(0);
        uint32_t original_data_offset = static_cast<uint32_t>(original_block_offset>>32);
        uint32_t original_delta_offset = static_cast<uint32_t>(original_block_offset&SIZE2MASK);
        uint32_t new_data_offset = original_data_offset;
        uint32_t new_delta_offset = original_delta_offset+ENTRY_DELTA_SIZE;
        if((new_delta_offset+new_data_offset)>block_size)[[unlikely]]{
            current_block->set_offset(original_block_offset);
            recovery_edge_deltas_block_consolidation(label_entry);
            return redo_log_operation(op);
        }
        if(previous_delta){
            previous_delta->invalidate_ts.store(op.timestamp,std::memory_order_release);
        }
        if(op.type==WALType::EDGE_UPDATE){
            char* data = (char*)(&op.e_data);
            /*auto append_result = current_block->checked_append_edge_delta(op.dst,op.timestamp,EdgeDeltaType::INSERT_DELTA,data,8,current_delta_chain_head_offset,previous_delta_offset);
            if(append_result.first!=EdgeDeltaInstallResult::SUCCESS||!append_result.second)[[unlikely]]{
                throw std::runtime_error("error, install insert edge delta");
            }
            //std::cout<<"loading edge "<<v<<" "<<edge.dst<<std::endl;
            label_entry->delta_chain_index->at(delta_chain_id).update_offset(append_result.second);*/
            current_block->checked_append_edge_delta(op.dst,op.timestamp,EdgeDeltaType::INSERT_DELTA,data,8,current_delta_chain_head_offset,previous_delta_offset,new_delta_offset,new_data_offset);
            label_entry->delta_chain_index->at(delta_chain_id).update_offset(new_delta_offset);
        }else if(op.type==WALType::EDGE_DELETE){
            /*auto append_result = current_block->checked_append_edge_delta(op.dst,op.timestamp,EdgeDeltaType::DELETE_DELTA,nullptr,0,current_delta_chain_head_offset,previous_delta_offset);
            if(append_result.first!=EdgeDeltaInstallResult::SUCCESS||!append_result.second)[[unlikely]]{
                throw std::runtime_error("error, install delete edge delta");
            }
            //std::cout<<"loading edge "<<v<<" "<<edge.dst<<std::endl;
            label_entry->delta_chain_index->at(delta_chain_id).update_offset(append_result.second);*/
            current_block->checked_append_edge_delta(op.dst,op.timestamp,EdgeDeltaType::DELETE_DELTA,nullptr,0,current_delta_chain_head_offset,previous_delta_offset,new_delta_offset,new_data_offset);
            label_entry->delta_chain_index->at(delta_chain_id).update_offset(new_delta_offset);
        }else{
            assert(false);
        }
    }
}

void BwGraph::replay_log_file(std::string &path, GTX::timestamp_t max_checkpoint_ts) {
    //do log load and analysis
    std::ifstream file(path, std::ios::binary | std::ios::ate);
    std::streamsize size = file.tellg();
    file.seekg(0, std::ios::beg);
    std::vector<char> buffer(size);
    uint64_t current_offset =0;
    std::unordered_map<vertex_t,std::vector<log_operation>*> operations_by_vertex;
    std::vector<std::vector<log_operation>*> operation_vector;
    if (file.read(buffer.data(), size))
    {
        //analyze the log
        while(current_offset<size){
            assert(buffer[current_offset]=='s');
            current_offset++;
            assert(buffer[current_offset]=='t');
            current_offset++;
            assert(buffer[current_offset]=='a');
            current_offset++;
            assert(buffer[current_offset]=='r');
            current_offset++;
            assert(buffer[current_offset]=='t');
            current_offset++;
            //check ts
            uint64_t timestamp = *reinterpret_cast<uint64_t*>(buffer.data()+current_offset);
            //std::cout<<"timestamp is "<<timestamp<<std::endl;
            /*if(timestamp<max_checkpoint_ts){
                break;
            }*/
            current_offset+=8;
            /*assert(buffer[current_offset]=='v');
            current_offset++;
            assert(buffer[current_offset]=='u');*/

            while(true){
                if(buffer[current_offset]=='v'){
                    current_offset++;
                    uint64_t vid;
                    if(buffer[current_offset]=='u'){
                        //std::cout<<"vu"<<std::endl;
                        current_offset++;
                        vid = *reinterpret_cast<uint64_t*>(buffer.data()+current_offset);
                        //std::cout<<"src is "<<vid<<std::endl;
                        current_offset+=8;
                        uint64_t real_vid = *reinterpret_cast<uint64_t*>(buffer.data()+current_offset);
                       // std::cout<<"external vid is "<<real_vid<<std::endl;
                        current_offset+=8;
                        if(timestamp>=max_checkpoint_ts){
                            auto emplace_result = operations_by_vertex.try_emplace(vid, nullptr);
                            if(emplace_result.second){
                                emplace_result.first->second = new std::vector<log_operation>();
                                operation_vector.emplace_back(emplace_result.first->second);
                            }
                            emplace_result.first->second->emplace_back(WALType::VERTEX_UPDATE,vid,0,real_vid,0,0,timestamp);
                        }
                    }else{
                        assert(buffer[current_offset]=='d');
                       // std::cout<<"vd"<<std::endl;
                        current_offset++;
                        vid = *reinterpret_cast<uint64_t*>(buffer.data()+current_offset);
                        if(timestamp>=max_checkpoint_ts){
                            auto emplace_result = operations_by_vertex.try_emplace(vid, nullptr);
                            if(emplace_result.second){
                                emplace_result.first->second = new std::vector<log_operation>();
                                operation_vector.emplace_back(emplace_result.first->second);
                            }
                            emplace_result.first->second->emplace_back(WALType::VERTEX_DELETE,vid,0,std::numeric_limits<uint64_t>::max(),0,0,timestamp);
                        }
                      //  std::cout<<"src is "<<vid<<std::endl;
                    }
                }else if(buffer[current_offset]=='e') {
                    current_offset++;
                    if(buffer[current_offset]=='u'){
                        current_offset++;
                       // std::cout<<"eu"<<std::endl;
                        uint16_t label = *reinterpret_cast<uint16_t*>(buffer.data()+current_offset);
                        current_offset+=2;
                        uint64_t src = *reinterpret_cast<uint64_t*>(buffer.data()+current_offset);
                        current_offset+=8;
                        uint64_t dst = *reinterpret_cast<uint64_t*>(buffer.data()+current_offset);
                        current_offset+=8;
                        double weight = *reinterpret_cast<double*>(buffer.data()+current_offset);
                        current_offset+=8;
                        if(timestamp>=max_checkpoint_ts){
                            auto emplace_result = operations_by_vertex.try_emplace(src, nullptr);
                            if(emplace_result.second){
                                emplace_result.first->second = new std::vector<log_operation>();
                                operation_vector.emplace_back(emplace_result.first->second);
                            }
                            emplace_result.first->second->emplace_back(WALType::EDGE_UPDATE,src,dst,0,weight,label,timestamp);
                        }
                       // std::cout<<"src is "<<src<<" dest is "<<dst<<" label is "<<label <<" weight is "<<weight<<std::endl;
                    }else if(buffer[current_offset]=='d'){
                        current_offset++;
                       // std::cout<<"ed"<<std::endl;
                        uint16_t label = *reinterpret_cast<uint16_t*>(buffer.data()+current_offset);
                        current_offset+=2;
                        uint64_t src = *reinterpret_cast<uint64_t*>(buffer.data()+current_offset);
                        current_offset+=8;
                        uint64_t dst = *reinterpret_cast<uint64_t*>(buffer.data()+current_offset);
                        current_offset+=8;
                        if(timestamp>=max_checkpoint_ts){
                            auto emplace_result = operations_by_vertex.try_emplace(src, nullptr);
                            if(emplace_result.second){
                                emplace_result.first->second = new std::vector<log_operation>();
                                operation_vector.emplace_back(emplace_result.first->second);
                            }
                            emplace_result.first->second->emplace_back(WALType::EDGE_DELETE,src,dst,0,0,label,timestamp);
                        }
                       // std::cout<<"src is "<<src<<" dest is "<<dst<<" label is "<<label<<std::endl;
                    }else{
                        assert(buffer[current_offset]=='n');
                        current_offset++;
                        current_offset++;
                       // std::cout<<"commit group is over"<<std::endl;
                        break;
                    }
                }else{
                    assert(false);
                }
            }
            //break;
        }
    }
    buffer.clear();
    file.close();
    std::cout<<"redo log file "<<path<<" with "<<operation_vector.size()<<" vertices"<<std::endl;
#pragma omp parallel for
    for(uint64_t i=0; i< operation_vector.size();i++){
        auto* op_list = operation_vector[i];
        auto source_vid = op_list->at(0).src;
        for(auto it : *op_list){
            assert(it.src==source_vid);
            //do operation
            if(it.timestamp<max_checkpoint_ts){
                continue;
            }
            redo_log_operation(it);
        }
        delete op_list;
    }
}

void BwGraph::replay_log(GTX::timestamp_t max_checkpoint_ts) {
    std::string log_directory_path = "/scratch1/zhou822/GTX_Durability/log_directory/";
    std::vector<timestamp_t> log_file_timestamps;
    //load all file name sorted by timestamps, but we will do wal.log first
    for (const auto & entry : std::filesystem::directory_iterator(log_directory_path)){
        if(entry.path().string().find("wal.log")==std::string::npos){
            //get the ts from the name
            uint64_t index = 0;
            for(uint64_t i = entry.path().string().size()-1; i>=0; i--){
                if(entry.path().string().c_str()[i]=='/'){
                    index = i;
                    break;
                }
            }
            uint64_t timestamp = std::stoul(entry.path().string().substr(index+5));
            log_file_timestamps.emplace_back(timestamp);
        }
    }
    std::sort(log_file_timestamps.begin(),log_file_timestamps.end(),std::greater<>());
    //redo wal.log
    auto wal_path = log_directory_path;
    wal_path.append("wal.log");
    replay_log_file(wal_path,max_checkpoint_ts);
    //start processing by one file at a time
    for(auto ts : log_file_timestamps){
        if(ts<max_checkpoint_ts){
            break;
        }
        wal_path=log_directory_path;
        wal_path.append("wal_");
        wal_path.append(std::to_string(ts));
        replay_log_file(wal_path,max_checkpoint_ts);
    }
    std::cout<<"log replay finished"<<std::endl;
}
void BwGraph::allocate_max_vid(GTX::vertex_t max_vid) {
    vertex_t current_vid =0;
    while(current_vid<max_vid){
        current_vid = vertex_index.get_next_vid();
        auto& vertex_index_entry = vertex_index.get_vertex_index_entry(current_vid);
        //allocate an initial label block
        vertex_index_entry.valid.store(true,std::memory_order_release);
        vertex_index_entry.edge_label_block_ptr = block_manager.alloc(size_to_order(sizeof(EdgeLabelBlock)));
        auto edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
        edge_label_block->fill_information(current_vid,&block_manager);
    }
}
BwLabelEntry* BwGraph::allocate_label_edge_deltas_block(vertex_t vid,GTX::label_t label, GTX::order_t order) {
    auto& vertex_index_entry = get_vertex_index_entry(vid);
    //cannot insert to invalid vertex entry
    auto edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
    auto* edge_label_entry = edge_label_block->bulk_loader_lookup_label(label,&txn_tables,order);
    return edge_label_entry;
}

void BwGraph::recovery_edge_deltas_block_consolidation(GTX::BwLabelEntry *label_entry) {
    auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(label_entry->block_ptr);
    order_t new_order = current_block->get_order();
    new_order++;
    //std::unordered_set<vertex_t> seen_edges;
    auto new_block_ptr = block_manager.alloc(new_order);
    auto new_block = block_manager.convert<EdgeDeltaBlockHeader>(new_block_ptr);
    new_block->fill_metadata(current_block->get_owner_id(),0, 0, 0,new_order, &txn_tables,label_entry->delta_chain_index);
    label_entry->delta_chain_index->clear();
    label_entry->delta_chain_index->resize(new_block->get_delta_chain_num());
    auto current_offset = current_block->get_delta_offset_from_combined_offset(current_block->get_current_offset());
    auto edge_delta = current_block->get_edge_delta(current_offset);
    while(current_offset>0){
        if(edge_delta->invalidate_ts.load(std::memory_order_acquire)!=0){
            auto delta_chain_id = new_block->get_delta_chain_id(edge_delta->toID);
            auto& delta_chains_index_entry = label_entry->delta_chain_index->at(delta_chain_id);
            auto index_offset = delta_chains_index_entry.get_offset();
            auto append_result = new_block->checked_append_edge_delta(edge_delta->toID,edge_delta->creation_ts.load(),edge_delta->delta_type,edge_delta->data,edge_delta->data_length,index_offset,0);
            if(append_result.first!=EdgeDeltaInstallResult::SUCCESS||!append_result.second)[[unlikely]]{
                throw std::runtime_error("error, install edge delta");
            }
            //std::cout<<"loading edge "<<v<<" "<<edge.dst<<std::endl;
            delta_chains_index_entry.update_offset(append_result.second);
        }
        current_offset-=ENTRY_DELTA_SIZE;
    }
    uint8_t * ptr = block_manager.convert<uint8_t>(label_entry->block_ptr);
    auto original_order = current_block->get_order();
    memset(ptr,'\0',1ul<<original_order);//zero out memory I used
    block_manager.free(label_entry->block_ptr,original_order);
    label_entry->block_ptr = new_block_ptr;
}
#endif