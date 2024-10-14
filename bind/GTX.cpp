//
// Created by zhou822 on 6/17/23.
//

#include "GTX.hpp"
#include "core/bwgraph_include.hpp"
#include "core/algorithm.hpp"
#include <omp.h>
#include <charconv>

using namespace gt;
namespace impl = GTX;

Graph:: ~Graph(){
    commit_server_shutdown();
    commit_manager_worker.join();
    if(bfs){
        delete bfs;
    }
    if(pagerank){
        delete pagerank;
    }
    if(sssp){
        delete sssp;
    }
}
Graph::Graph(std::string block_path, size_t _max_block_size, std::string wal_path) :graph(std::make_unique<impl::BwGraph>(block_path, _max_block_size, wal_path)){
    commit_manager_worker =std::thread(&Graph::commit_server_start, this);
}

vertex_t Graph::get_max_allocated_vid() {return graph->get_max_allocated_vid();}

RWTransaction Graph::begin_read_write_transaction() {
    return std::make_unique<impl::RWTransaction>(graph->begin_read_write_transaction());
}

ROTransaction Graph::begin_read_only_transaction() {
   // std::cout<<"ro"<<std::endl;
    return std::make_unique<impl::ROTransaction>(graph->begin_read_only_transaction());
}

gt::SharedROTransaction Graph::begin_shared_read_only_transaction() {
    //graph->get_thread_manager().print_debug_stats();
    return {std::make_unique<impl::SharedROTransaction>(graph->begin_shared_ro_transaction()),this};
}

GTX::EdgeDeltaBlockHeader *Graph::get_edge_block(gt::vertex_t vid, gt::label_t l) {
    auto& index_entry = graph->get_vertex_index_entry(vid);
    auto target_label_block = graph->get_block_manager().convert<GTX::EdgeLabelBlock>(index_entry.edge_label_block_ptr);
    GTX::BwLabelEntry* target_label_entry;
    auto result = target_label_block->reader_lookup_label(l,target_label_entry);
    return graph->get_block_manager().convert<GTX::EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
}
void Graph::commit_server_start() {
#if ENSURE_DURABILITY
    graph->get_commit_manager().wal_server_loop();
    //graph->get_commit_manager().asynchronous_wal_loop();
    //graph->get_commit_manager().simplified_asynchronous_wal_loop();
#else
    graph->get_commit_manager().server_loop();
#endif
}
void Graph::commit_server_shutdown() {
    graph->get_commit_manager().shutdown_signal();
}

uint8_t Graph::get_worker_thread_id() {
    return graph->get_worker_thread_id();
}

void Graph::print_thread_id_allocation() {
    graph->get_thread_manager().print_debug_stats();
}
void Graph::execute_manual_checking(gt::vertex_t vid) {
    graph->execute_manual_delta_block_checking(vid);
}
bool Graph::is_txn_table_empty() {
    auto& txn_tables = graph->get_txn_tables();
    for(auto i=0; i<worker_thread_num-1; i++){
        if(!txn_tables.get_table(i).is_empty()){
            return false;
        }
    }
    return true;
}

void Graph::print_garbage_queue_status() {
    graph->print_garbage_status();
}
void Graph::thread_exit() {
    graph->thread_exit();
}

void Graph::garbage_clean() {
    graph->garbage_clean();
}
void Graph::force_consolidation_clean() {
    graph->force_consolidation_clean();
}

void Graph::set_worker_thread_num(uint64_t new_size) {
    graph->set_worker_thread_num(new_size);
}
/*
 * sets the writer number in the mixed workload
 */
void Graph::set_writer_thread_num(uint64_t writer_num){
    graph->set_writer_thread_num(writer_num);
}

void Graph::on_finish_loading(){
    graph->on_finish_loading();
}
/*
 * to be cached by the reader
 */
uint8_t Graph::get_openmp_worker_thread_id() {
    return graph->get_openmp_worker_thread_id();
}

void Graph::print_and_clear_txn_stats() {
#if TRACK_COMMIT_ABORT
    graph->print_and_clear_txn_stats();
#endif
#if TRACK_GARBAGE_RECORD_TIME
    graph->get_block_manager().print_avg_alloc_time();
    graph->get_block_manager().print_avg_free_time();
    graph->get_block_manager().print_avg_garbage_record_time();
#endif
}

void Graph::on_openmp_txn_start(uint64_t read_ts) {
    graph->on_openmp_transaction_start(read_ts);
}

void Graph::on_openmp_section_finishing() {
    graph->on_openmp_parallel_session_finish();
}

void Graph::manual_commit_server_shutdown() {
    graph->get_commit_manager().shutdown_signal();
    commit_manager_worker.join();
}

void Graph::manual_commit_server_restart() {
    graph->get_commit_manager().restart();
    commit_manager_worker =std::thread(&Graph::commit_server_start, this);
}

void Graph::eager_consolidation_on_edge_delta_block(vertex_t vid, label_t label) {
    graph->eager_consolidation_on_edge_delta_block(vid,label);
}

void Graph::whole_label_graph_eager_consolidation(gt::label_t label) {
    auto max_vid = graph->get_max_allocated_vid();
#pragma omp parallel for
    for(vertex_t i=1; i<=max_vid;i++){
        graph->eager_consolidation_on_edge_delta_block(i,label);
    }
}

void Graph::configure_distinct_readers_and_writers(uint64_t reader_count, uint64_t writer_count) {
    manual_commit_server_shutdown();
    graph->configure_distinct_readers_and_writers(reader_count,writer_count);
    manual_commit_server_restart();
}

void Graph::on_openmp_workloads_finish() {
    graph->on_openmp_workloads_finish();
}

void Graph::recovery_from_checkpoint() {
#if ENSURE_DURABILITY
    graph->load_checkpoint();
#endif
}

//for algorithms
PageRankHandler Graph::get_pagerank_handler(uint64_t num) {
    return {std::make_unique<impl::PageRank>(graph.get(),num)};
}

BFSHandler Graph::get_bfs_handler(uint64_t num) {
    return {std::make_unique<impl::BFS>(graph.get(),num)};
}

SSSPHandler Graph::get_sssp_handler(uint64_t num) {
    return {std::make_unique<GTX::SSSP>(graph.get(),num)};
}

TwoHopNeighborsHandler Graph::get_two_hop_neighbors_handler() {
    return {std::make_unique<GTX::TwoHopNeighbors>(graph.get())};
}

OneHopNeighborsHandler Graph::get_one_hop_neighbors_handler() {
    return {std::make_unique<GTX::OneHopNeighbors>(graph.get())};
}

std::vector<std::pair<uint64_t,int64_t>>* Graph::compute_bfs(uint64_t max_vid,uint64_t root, int alpha, int beta) {
    if(!bfs)[[unlikely]]{
        bfs = new impl::BFS(this->graph.get(),max_vid);
    }
    bfs->bfs(root,alpha,beta);
    return bfs->get_result();
}

std::vector<std::pair<uint64_t,double>>* Graph::compute_pagerank(uint64_t num_vertices, uint64_t num_iterations, double damping_factor) {
    if(!pagerank)[[unlikely]]{
        pagerank = new impl::PageRank(this->graph.get(),num_vertices);
    }
    pagerank->compute_pagerank(num_iterations,damping_factor);
    return pagerank->get_result();
}

std::vector<std::pair<uint64_t, double>>* Graph::compute_sssp(uint64_t max_vid, uint64_t source, double delta) {
    if(sssp)[[likely]]{
        sssp->reset();
    }else[[unlikely]]{
        sssp = new impl::SSSP(this->graph.get(),max_vid);
    }
    sssp->compute(source,delta);
    return sssp->get_result();
}

//read only transactions
ROTransaction::ROTransaction(std::unique_ptr<GTX::ROTransaction> _txn) :txn(std::move(_txn)){}

ROTransaction:: ~ROTransaction() = default;

std::string_view ROTransaction::get_vertex(gt::vertex_t src) {return txn->get_vertex(src);}

std::string_view ROTransaction::get_edge(gt::vertex_t src, gt::vertex_t dst, gt::label_t label) {
   // std::cout<<"ro"<<std::endl;
    while(true){
        auto result = txn->get_edge(src,dst,label);
        if(result.first==GTX::Txn_Operation_Response::SUCCESS)[[likely]]{
            return result.second;
        }
#if ENABLE_VERTEX_DELETION
        else if(result.first==GTX::Txn_Operation_Response::FAIL)[[unlikely]]{
            throw VertexDeletedException("vertex has been deleted");
        }
#endif
    }
}

double ROTransaction::get_edge_weight(gt::vertex_t src, gt::vertex_t dst, gt::label_t label) {
    double* weight = nullptr;
    while(true){
        auto result = txn->get_edge_weight(src,label,dst,weight);
        if(result == GTX::Txn_Operation_Response::SUCCESS)[[likely]]{
            if(weight)[[likely]]{
                return *weight;
            }else{
                return std::numeric_limits<double>::signaling_NaN();
            }
        }
#if ENABLE_VERTEX_DELETION
        else if(result==GTX::Txn_Operation_Response::FAIL)[[unlikely]]{
            throw VertexDeletedException("vertex has been deleted");
        }
#endif
    }
}

EdgeDeltaIterator ROTransaction::get_edges(gt::vertex_t src, gt::label_t label) {
   // std::cout<<"ro"<<std::endl;
    while(true){
        auto result = txn->get_edges(src,label);
        if(result.first==GTX::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::EdgeDeltaIterator>(result.second);
        }
#if ENABLE_VERTEX_DELETION
        else if(result.first==GTX::Txn_Operation_Response::FAIL)[[unlikely]]{
            throw VertexDeletedException("vertex has been deleted");
        }
#endif
    }
}

SimpleEdgeDeltaIterator ROTransaction::simple_get_edges(gt::vertex_t src, gt::label_t label) {
    while(true){
        auto result = txn->simple_get_edges(src,label);
        if(result.first==GTX::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::SimpleEdgeDeltaIterator>(result.second);
        }
#if ENABLE_VERTEX_DELETION
        else if(result.first==GTX::Txn_Operation_Response::FAIL)[[unlikely]]{
            throw VertexDeletedException("vertex has been deleted");
        }
#endif
    }
}

void ROTransaction::commit() {txn->commit();}

SharedROTransaction::SharedROTransaction(std::unique_ptr<GTX::SharedROTransaction> _txn, Graph* source): txn(std::move(_txn)), graph(source) {}

SharedROTransaction::~SharedROTransaction() = default;

void SharedROTransaction::commit() {txn->commit();}

uint64_t SharedROTransaction::get_read_timestamp() {
    return txn->get_read_ts();
}

std::string_view SharedROTransaction::static_get_vertex(gt::vertex_t src) {
    return txn->static_get_vertex(src);
}

std::string_view SharedROTransaction::static_get_edge(gt::vertex_t src, gt::vertex_t dst, gt::label_t label) {
    return txn->static_get_edge(src,dst,label);
}

StaticEdgeDeltaIterator SharedROTransaction::static_get_edges(gt::vertex_t src, gt::label_t label) {
    return std::make_unique<impl::StaticEdgeDeltaIterator>(txn->static_get_edges(src,label));
}
void  SharedROTransaction::static_get_edges(vertex_t src, label_t label, StaticEdgeDeltaIterator& edge_iterator){
    edge_iterator.clear();
    txn->static_get_edges(src,label,edge_iterator.iterator);
}
std::string_view SharedROTransaction::get_vertex(gt::vertex_t src) {
    return txn->get_vertex(src);
}
std::string_view SharedROTransaction::get_vertex(gt::vertex_t src, uint8_t thread_id) {
    return txn->get_vertex(src,thread_id);
}
std::string_view SharedROTransaction::get_edge(gt::vertex_t src, gt::vertex_t dst, gt::label_t label) {
    while(true){
        auto result = txn->get_edge(src,dst,label);
        if(result.first==GTX::Txn_Operation_Response::SUCCESS){
            return result.second;
        }
#if ENABLE_VERTEX_DELETION
        else if(result.first==GTX::Txn_Operation_Response::FAIL)[[unlikely]]{
            throw VertexDeletedException("vertex has been deleted");
        }
#endif
    }
}

void SharedROTransaction::thread_on_openmp_section_finish(uint8_t thread_id) {
    txn->thread_on_openmp_section_finish(thread_id);
}
std::string_view
SharedROTransaction::get_edge(gt::vertex_t src, gt::vertex_t dst, gt::label_t label, uint8_t thread_id) {
    while (true) {
        auto result = txn->get_edge(src, dst, label, thread_id);
        if (result.first == GTX::Txn_Operation_Response::SUCCESS) {
            return result.second;
        }else if(result.first==GTX::Txn_Operation_Response::FAIL)[[unlikely]]{
            throw VertexDeletedException("vertex has been deleted");
        }
    }
}

SimpleEdgeDeltaIterator SharedROTransaction::generate_edge_delta_iterator(uint8_t thread_id) {
    return std::make_unique<impl::SimpleEdgeDeltaIterator>(txn->generate_edge_iterator(thread_id));
}

StaticEdgeDeltaIterator SharedROTransaction::generate_static_edge_delta_iterator(){
    return std::make_unique<impl::StaticEdgeDeltaIterator>(txn->generate_static_edge_iterator());
}

EdgeDeltaIterator SharedROTransaction::get_edges(gt::vertex_t src, gt::label_t label, uint8_t thread_id) {
    while(true){
        auto result = txn->get_edges(src,label, thread_id);
        if(result.first==GTX::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::EdgeDeltaIterator>(result.second);
        }
#if ENABLE_VERTEX_DELETION
        else if(result.first==GTX::Txn_Operation_Response::FAIL)[[unlikely]]{
            throw VertexDeletedException("vertex has been deleted");
        }
#endif
    }
}
EdgeDeltaIterator SharedROTransaction::get_edges(gt::vertex_t src, gt::label_t label) {
    while(true){
        auto result = txn->get_edges(src,label);
        if(result.first==GTX::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::EdgeDeltaIterator>(result.second);
        }
#if ENABLE_VERTEX_DELETION
        else if(result.first==GTX::Txn_Operation_Response::FAIL)[[unlikely]]{
            throw VertexDeletedException("vertex has been deleted");
        }
#endif
    }
}
SimpleEdgeDeltaIterator SharedROTransaction::simple_get_edges(gt::vertex_t src, gt::label_t label) {
    while(true){
        auto result = txn->simple_get_edges(src,label);
        if(result.first==GTX::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::SimpleEdgeDeltaIterator>(result.second);
        }
#if ENABLE_VERTEX_DELETION
        else if(result.first==GTX::Txn_Operation_Response::FAIL)[[unlikely]]{
            throw VertexDeletedException("vertex has been deleted");
        }
#endif
    }
}
SimpleEdgeDeltaIterator SharedROTransaction::simple_get_edges(gt::vertex_t src, gt::label_t label, uint8_t thread_id) {
    while(true){
        auto result = txn->simple_get_edges(src,label,thread_id);
        if(result.first==GTX::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::SimpleEdgeDeltaIterator>(result.second);
        }
#if ENABLE_VERTEX_DELETION
        else if(result.first==GTX::Txn_Operation_Response::FAIL)[[unlikely]]{
            throw VertexDeletedException("vertex has been deleted");
        }
#endif
    }
}

void SharedROTransaction::simple_get_edges(gt::vertex_t src, gt::label_t label, uint8_t thread_id,
                                           SimpleEdgeDeltaIterator &edge_iterator) {
    while(true){
        auto result = txn->simple_get_edges(src,label,thread_id,edge_iterator.iterator);
        if(result == GTX::Txn_Operation_Response::SUCCESS){
            return;
        }
#if ENABLE_VERTEX_DELETION
        else if(result==GTX::Txn_Operation_Response::FAIL)[[unlikely]]{
            throw VertexDeletedException("vertex has been deleted");
        }
#endif
    }
}


void SharedROTransaction::print_debug_info() {
    std::cout<<"Shared RO Transaction printing debug info"<<std::endl;
    graph->print_thread_id_allocation();
}
Graph *SharedROTransaction::get_graph() {
    return graph;
}
//read-write transactions
RWTransaction::~RWTransaction() = default;

RWTransaction::RWTransaction(std::unique_ptr<GTX::RWTransaction> _txn):txn(std::move(_txn)) {}

vertex_t RWTransaction::new_vertex() {return txn->create_vertex();}

void RWTransaction::put_vertex(gt::vertex_t vertex_id, std::string_view data) {
#if TRACK_EXECUTION_TIME
    auto start = std::chrono::high_resolution_clock::now();
#endif
    auto result = txn->update_vertex(vertex_id,data);
#if TRACK_EXECUTION_TIME
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    txn->get_graph().local_thread_vertex_write_time.local()+= duration.count();
#endif
    if(result==GTX::Txn_Operation_Response::SUCCESS){
#if ENSURE_DURABILITY
        txn->record_wal(GTX::WALType::VERTEX_UPDATE,vertex_id,data);
#endif
        return;
    }else{
        throw RollbackExcept("write write conflict vertex");
    }
}

void RWTransaction::put_edge(gt::vertex_t src, gt::label_t label, gt::vertex_t dst, std::string_view edge_data) {
    while(true){
#if TRACK_EXECUTION_TIME
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto result = txn->put_edge(src,dst,label,edge_data);
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        txn->get_graph().local_thread_edge_write_time.local()+= duration.count();
#endif
        if(result == GTX::Txn_Operation_Response::SUCCESS){
            break;
        }else if(result ==GTX::Txn_Operation_Response::FAIL){
            throw RollbackExcept("write write conflict edge");
        }
    }
#if DIRECTED_GRAPH
    while(true){
#if TRACK_EXECUTION_TIME
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto result = txn->put_edge(dst,src,label,edge_data);
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        txn->get_graph().local_thread_edge_write_time.local()+= duration.count();
#endif
        if(result == GTX::Txn_Operation_Response::SUCCESS){
            return;
        }else if(result ==GTX::Txn_Operation_Response::FAIL){
            throw RollbackExcept("write write conflict edge");
        }
    }
#endif
}

bool
RWTransaction::checked_put_edge(gt::vertex_t src, gt::label_t label, gt::vertex_t dst, std::string_view edge_data) {
    bool return_result = true;
    bool continue_executing = true;
    while(continue_executing){
#if TRACK_EXECUTION_TIME
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto result = txn->checked_put_edge(src,dst,label,edge_data);
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        txn->get_graph().local_thread_edge_write_time.local()+= duration.count();
#endif
        if(result == GTX::Txn_Operation_Response::SUCCESS_NEW_DELTA){
#if ENSURE_DURABILITY
            txn->record_wal(GTX::WALType::EDGE_UPDATE,src,edge_data,dst,label);
#endif
            //return true;
            //break;
            continue_executing= false;
        }else if(result == GTX::Txn_Operation_Response::SUCCESS_EXISTING_DELTA){
#if ENSURE_DURABILITY
            txn->record_wal(GTX::WALType::EDGE_UPDATE,src,edge_data,dst,label);
#endif
            //return false;
            return_result = false;
            continue_executing= false;
        }
        else if(result ==GTX::Txn_Operation_Response::FAIL){
#if TRACK_COMMIT_ABORT
            txn->get_graph().register_abort();
#endif
            throw RollbackExcept("write write conflict edge");
        }
#if TRACK_COMMIT_ABORT
        txn->get_graph().register_loop();
#endif
    }

#if DIRECTED_GRAPH
    continue_executing= true;
    while(continue_executing){
#if TRACK_EXECUTION_TIME
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto result = txn->checked_put_edge(dst,src,label,edge_data);
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        txn->get_graph().local_thread_edge_write_time.local()+= duration.count();
#endif
        if(result ==  GTX::Txn_Operation_Response::SUCCESS_NEW_DELTA || result == GTX::Txn_Operation_Response::SUCCESS_EXISTING_DELTA){
#if ENSURE_DURABILITY
            txn->record_wal(GTX::WALType::EDGE_UPDATE,dst,edge_data,src,label);
#endif
            return return_result;
        }
        else if (result == GTX::Txn_Operation_Response::FAIL){
#if TRACK_COMMIT_ABORT
            txn->get_graph().register_abort();
#endif
            throw RollbackExcept("write write conflict edge");
        }
#if TRACK_COMMIT_ABORT
        txn->get_graph().register_loop();
#endif
    }
#endif
}

 bool RWTransaction::checked_single_put_edge(vertex_t src, label_t label, vertex_t dst, std::string_view edge_data){
     bool return_result = true;
    while(true){
#if TRACK_EXECUTION_TIME
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto result = txn->checked_single_put_edge(src,dst,label,edge_data);
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        txn->get_graph().local_thread_edge_write_time.local()+= duration.count();
#endif
        if(result == GTX::Txn_Operation_Response::SUCCESS_NEW_DELTA){
#if ENSURE_DURABILITY
            txn->record_wal(GTX::WALType::EDGE_UPDATE,src,edge_data,dst,label);
#endif
            //return true;
            break;
        }else if(result == GTX::Txn_Operation_Response::SUCCESS_EXISTING_DELTA){
#if ENSURE_DURABILITY
            txn->record_wal(GTX::WALType::EDGE_UPDATE,src,edge_data,dst,label);
#endif
            //return false;
            return_result = false;
            break;
        }
        else if(result ==GTX::Txn_Operation_Response::FAIL){
#if TRACK_COMMIT_ABORT
            txn->get_graph().register_abort();
#endif
            throw RollbackExcept("write write conflict edge");
        }
#if TRACK_COMMIT_ABORT
        txn->get_graph().register_loop();
#endif
    }

#if DIRECTED_GRAPH
    while(true){
#if TRACK_EXECUTION_TIME
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto result = txn->checked_single_put_edge(dst,src,label,edge_data);
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        txn->get_graph().local_thread_edge_write_time.local()+= duration.count();
#endif
        if(result ==  GTX::Txn_Operation_Response::SUCCESS_NEW_DELTA || result == GTX::Txn_Operation_Response::SUCCESS_EXISTING_DELTA){
#if ENSURE_DURABILITY
            txn->record_wal(GTX::WALType::EDGE_UPDATE,dst,edge_data,src,label);
#endif
            return return_result;
        }
        else if (result == GTX::Txn_Operation_Response::FAIL){
#if TRACK_COMMIT_ABORT
            txn->get_graph().register_abort();
#endif
            throw RollbackExcept("write write conflict edge");
        }
#if TRACK_COMMIT_ABORT
        txn->get_graph().register_loop();
#endif
    }
#endif

 }
void RWTransaction::delete_edge(gt::vertex_t src, gt::label_t label, gt::vertex_t dst) {
    while(true){
#if TRACK_EXECUTION_TIME
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto result = txn->delete_edge(src,dst,label);
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        txn->get_graph().local_thread_edge_write_time.local()+= duration.count();
#endif
        if(result == GTX::Txn_Operation_Response::SUCCESS){
#if ENSURE_DURABILITY
            txn->record_wal(GTX::WALType::EDGE_DELETE,src,std::string_view(),dst,label);
#endif
            return;
        }else if(result ==GTX::Txn_Operation_Response::FAIL){
            throw RollbackExcept("write write conflict edge");
        }
    }
}

bool RWTransaction::checked_delete_edge(gt::vertex_t src, gt::label_t label, gt::vertex_t dst) {
    bool return_result = true;
    bool continue_executing = true;
    while(continue_executing){
#if TRACK_EXECUTION_TIME
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto result = txn->checked_delete_edge(src,dst,label);
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        txn->get_graph().local_thread_edge_write_time.local()+= duration.count();
#endif
        if(result == GTX::Txn_Operation_Response::SUCCESS_EXISTING_DELTA){
#if ENSURE_DURABILITY
            txn->record_wal(GTX::WALType::EDGE_DELETE,src,std::string_view(),dst,label);
#endif
            //return true;
            //break;
            continue_executing = false;
        }else if(result == GTX::Txn_Operation_Response::SUCCESS_NEW_DELTA){
#if ENSURE_DURABILITY
            //nothing was deleted, so no wal needed
#endif
            //return false;
            return_result = false;
            continue_executing = false;
            //break;
        }
        else if(result ==GTX::Txn_Operation_Response::FAIL){
#if TRACK_COMMIT_ABORT
            txn->get_graph().register_abort();
#endif
            throw RollbackExcept("write write conflict edge");
        }
    }

#if DIRECTED_GRAPH
    continue_executing = true;
    while(continue_executing){
#if TRACK_EXECUTION_TIME
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto result = txn->checked_delete_edge(dst,src,label);
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        txn->get_graph().local_thread_edge_write_time.local()+= duration.count();
#endif
        if(result == GTX::Txn_Operation_Response::SUCCESS_EXISTING_DELTA){
#if ENSURE_DURABILITY
            txn->record_wal(GTX::WALType::EDGE_DELETE,dst,std::string_view(),src,label);
#endif
            //return true;
            return return_result;
        }else if(result == GTX::Txn_Operation_Response::SUCCESS_NEW_DELTA){
#if ENSURE_DURABILITY
            //nothing was deleted, so no wal needed
#endif
            //return false;
            return return_result;
        }
        else if(result ==GTX::Txn_Operation_Response::FAIL){
#if TRACK_COMMIT_ABORT
            txn->get_graph().register_abort();
#endif
            throw RollbackExcept("write write conflict edge");
        }
    }
#endif
}
std::string_view RWTransaction::get_vertex(gt::vertex_t src) {
    return txn->get_vertex(src);
}

std::string_view RWTransaction::get_edge(gt::vertex_t src, gt::vertex_t dst, gt::label_t label) {
    while(true){
        auto result = txn->get_edge(src,dst,label);
        if(result.first==GTX::Txn_Operation_Response::SUCCESS){
            return result.second;
        }else if(result.first == GTX::Txn_Operation_Response::FAIL){
            throw RollbackExcept("found write write conflict on previous write when reading edge");
        }
    }
}

EdgeDeltaIterator RWTransaction::get_edges(gt::vertex_t src, gt::label_t label) {
    while(true){
        auto result = txn->get_edges(src,label);
        if(result.first==GTX::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::EdgeDeltaIterator>(result.second);
        }else if(result.first == GTX::Txn_Operation_Response::FAIL){
            throw RollbackExcept("found write write conflict on previous write when scanning edges");
        }
    }
}

SimpleEdgeDeltaIterator RWTransaction::simple_get_edges(gt::vertex_t src, gt::label_t label) {
    while(true){
        auto result = txn->simple_get_edges(src,label);
        if(result.first==GTX::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::SimpleEdgeDeltaIterator>(result.second);
        }else if(result.first == GTX::Txn_Operation_Response::FAIL){
            throw RollbackExcept("found write write conflict on previous write when scanning edges");
        }
    }
}

bool RWTransaction::commit() {
#if USING_EAGER_COMMIT
    return txn->eager_commit();
#else
    return txn->commit();
#endif
}

void RWTransaction::abort() {
    txn->abort();
}

EdgeDeltaIterator::EdgeDeltaIterator(std::unique_ptr<GTX::EdgeDeltaIterator> _iter):iterator(std::move(_iter)) {}

EdgeDeltaIterator::~EdgeDeltaIterator() = default;

void EdgeDeltaIterator::close() {iterator->close();}

void EdgeDeltaIterator::next() {
    current_delta = iterator->next_delta();
}
bool EdgeDeltaIterator::valid() {
    next();
    return current_delta!= nullptr;
}
vertex_t EdgeDeltaIterator::dst_id() const {
    return current_delta->toID;
}

std::string_view EdgeDeltaIterator::edge_delta_data() const {
    if(current_delta->data_length<=16){
        return std::string_view (current_delta->data, current_delta->data_length);
    }else{
        return std::string_view (iterator->get_data(current_delta->data_offset),current_delta->data_length);
    }
}

//simple edge iterator
SimpleEdgeDeltaIterator::SimpleEdgeDeltaIterator(std::unique_ptr<GTX::SimpleEdgeDeltaIterator> _iter):iterator(std::move(_iter)) {}

SimpleEdgeDeltaIterator::~SimpleEdgeDeltaIterator() = default;

void SimpleEdgeDeltaIterator::close() {iterator->close();}

void SimpleEdgeDeltaIterator::next() {
    current_delta = iterator->next_delta();
}
void SimpleEdgeDeltaIterator::next_second_round() {
    current_delta = iterator->next_delta_second_round();
}
bool SimpleEdgeDeltaIterator::valid() {
    next();
    return current_delta!= nullptr;
}
bool SimpleEdgeDeltaIterator::valid_second_round() {
    next_second_round();
    return current_delta!= nullptr;
}
vertex_t SimpleEdgeDeltaIterator::dst_id() const {
    return current_delta->toID;
}

uint64_t SimpleEdgeDeltaIterator::get_vertex_degree() {
    return iterator->vertex_degree();
}

std::string_view SimpleEdgeDeltaIterator::edge_delta_data() const {
    if(current_delta->data_length<=16){
        return std::string_view (current_delta->data, current_delta->data_length);
    }else{
        return std::string_view (iterator->get_data(current_delta->data_offset),current_delta->data_length);
    }
}

double SimpleEdgeDeltaIterator::edge_delta_weight() const {
    return  *reinterpret_cast<double*>(current_delta->data);
}

StaticEdgeDeltaIterator::StaticEdgeDeltaIterator(std::unique_ptr<GTX::StaticEdgeDeltaIterator> _iter):iterator(std::move(_iter)) {}

StaticEdgeDeltaIterator::~StaticEdgeDeltaIterator() = default;

void StaticEdgeDeltaIterator::next() {
    current_delta = iterator->next_delta();
}

bool StaticEdgeDeltaIterator::valid() {
    next();
    return current_delta!= nullptr;
}

vertex_t StaticEdgeDeltaIterator::dst_id() const {
    return current_delta->toID;
}

std::string_view StaticEdgeDeltaIterator::edge_delta_data() const {
    if(current_delta->data_length<=16){
        return std::string_view (current_delta->data, current_delta->data_length);
    }else{
        return std::string_view (iterator->get_data(current_delta->data_offset),current_delta->data_length);
    }
}

uint32_t StaticEdgeDeltaIterator::vertex_degree(){
    return iterator->get_degree();
}
double StaticEdgeDeltaIterator::get_weight() {
    return *reinterpret_cast<double*>(current_delta->data);
}

PageRankHandler::PageRankHandler(std::unique_ptr<GTX::PageRank> _handler):pagerank(std::move(_handler)) {}

PageRankHandler::~PageRankHandler() = default;

void PageRankHandler::compute(uint64_t num_iterations, double damping_factor) {
    pagerank->compute_pagerank(num_iterations,damping_factor);
}

std::vector<double> *PageRankHandler::get_raw_result() {
    return pagerank->get_raw_result();
}

std::vector<std::pair<uint64_t, double>> *PageRankHandler::get_result() {
    return pagerank->get_result();
}

BFSHandler::BFSHandler(std::unique_ptr<GTX::BFS> _handler):bfs(std::move(_handler)) {}

BFSHandler::~BFSHandler()=default;

void BFSHandler::compute(uint64_t root, int alpha, int beta) {
    bfs->bfs(root,alpha,beta);
}

std::vector<int64_t>*BFSHandler::get_raw_result() {
    return bfs->get_raw_result();
}

std::vector<std::pair<uint64_t, int64_t>> *BFSHandler::get_result() {
    return bfs->get_result();
}

SSSPHandler::SSSPHandler(std::unique_ptr<GTX::SSSP> _handler):sssp(std::move(_handler)) {}

SSSPHandler::~SSSPHandler() = default;

void SSSPHandler::compute(uint64_t source, double delta) {
    sssp->compute(source,delta);
}

std::vector<std::pair<uint64_t, double>> *SSSPHandler::get_result() {
    return sssp->get_result();
}

OneHopNeighborsHandler::OneHopNeighborsHandler(std::unique_ptr<GTX::OneHopNeighbors> _handler):ohns(std::move(_handler)) {}

OneHopNeighborsHandler::~OneHopNeighborsHandler() = default;

void OneHopNeighborsHandler::compute(std::vector<uint64_t> &vertices) {
    ohns->find_one_hop_neighbors(vertices);
}

std::unordered_map<uint64_t, std::vector<uint64_t>> *OneHopNeighborsHandler::get_result() {
    return ohns->get_result();
}

TwoHopNeighborsHandler::TwoHopNeighborsHandler(std::unique_ptr<GTX::TwoHopNeighbors> _handler):thns(std::move(_handler)) {}

TwoHopNeighborsHandler::~TwoHopNeighborsHandler() = default;

void TwoHopNeighborsHandler::compute(std::vector<uint64_t> &vertices) {
    thns->find_two_hop_neighbors(vertices);
}

std::unordered_map<uint64_t, std::vector<uint64_t>> *TwoHopNeighborsHandler::get_result() {
    return thns->get_result();
}

DeleteTransaction::DeleteTransaction(std::unique_ptr<GTX::DeleteTransaction> _txn):txn(std::move(_txn)) {

}

DeleteTransaction::~DeleteTransaction() = default;

void DeleteTransaction::delete_vertex(vertex_t vid) {
    txn->delete_vertex(vid);
}