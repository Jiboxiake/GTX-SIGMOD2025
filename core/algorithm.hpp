//
// Created by zhou822 on 1/8/24.
//

#pragma once
#include "bw_index.hpp"
#include "gtx_transaction.hpp"
#include "transaction_tables.hpp"
#include "edge_iterator.hpp"
#include "previous_version_garbage_queue.hpp"
#include "types.hpp"
#include "edge_delta_block_state_protection.hpp"
#include <set>
#include "Libraries/gapbs.hpp"

namespace GTX{

    class BFS{
    public:
        BFS(BwGraph* input_graph, uint64_t input_num):graph(input_graph),num_vertices(input_num){
            max_vertex_id = graph->get_max_allocated_vid();
            txn_tables = &graph->get_txn_tables();
            distances.resize(max_vertex_id);
        }
        int64_t init_distance(SharedROTransaction& txn){
            std::atomic_int64_t total_edge_num = 0;
#pragma omp parallel
            {
                uint8_t thread_id = graph->get_openmp_worker_thread_id();
                int64_t private_edge_num = 0;
#pragma omp for
                for (uint64_t n = 1; n <= max_vertex_id; n++) {
                    auto out_degree = txn.get_neighborhood_size_signed(n,1,thread_id);
                    if(out_degree==std::numeric_limits<int64_t>::max())[[unlikely]]{
                        distances[n-1] = out_degree;
                    }else[[likely]]{
                        private_edge_num+=out_degree;
                        distances[n - 1] = out_degree != 0 ? -out_degree : -1;
                    }
                }
                total_edge_num.fetch_add(private_edge_num,std::memory_order_acq_rel);
                txn.thread_on_openmp_section_finish(thread_id);
            }
            graph->on_openmp_parallel_session_finish();
            return total_edge_num.load(std::memory_order_acquire)/2;
        }
        int64_t do_bfs_TDStep(SharedROTransaction& txn, int64_t distance, gapbs::SlidingQueue<int64_t>& queue){
            int64_t scout_count = 0;
#pragma omp parallel reduction(+ : scout_count)
            {
                uint8_t thread_id = graph->get_openmp_worker_thread_id();
                gapbs::QueueBuffer<int64_t> lqueue(queue);
#pragma omp for schedule(dynamic, 64)
                for (auto q_iter = queue.begin(); q_iter < queue.end(); q_iter++) {
                    int64_t u = *q_iter;
                    uint32_t current_delta_offset = 0;
                    auto current_block = txn.get_block_header(u, 1, thread_id, &current_delta_offset);
                    //determine which block to read
                    if(current_block->get_creation_time()<=read_ts)[[likely]]{//read latest block
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
                                if (txn_tables->get_status(original_ts, status))[[likely]] {
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
                                                txn_tables->reduce_op_count(original_ts,1);
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
                            if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                                uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                        std::memory_order_acquire);
                                //found the visible edge
                                if(original_ts<=read_ts&&(current_invalidation_ts==0||current_invalidation_ts>read_ts)){
                                    auto dst = current_delta->toID;
                                    int64_t curr_val = distances[dst-1];
                                    if (curr_val < 0 && gapbs::compare_and_swap(distances[dst-1], curr_val, distance)) {
                                        lqueue.push_back(dst);
                                        scout_count += -curr_val;
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
                        txn.unregister_thread_block_access(thread_id);
                    }else{//read previous block
                        txn.unregister_thread_block_access(thread_id);
                        bool found = false;
                        while (current_block->get_previous_ptr()) {
                            current_block = graph->get_block_manager().convert<EdgeDeltaBlockHeader>(
                                    current_block->get_previous_ptr());
                            if (read_ts >= current_block->get_creation_time())[[likely]] {
                                found = true;
                                break;
                            }
                        }
                        if(found)[[likely]]{
                            auto previous_block_offset = current_block->get_current_offset();
                            current_delta_offset = static_cast<uint32_t>(previous_block_offset & SIZE2MASK);
                            auto current_delta = current_block->get_edge_delta(current_delta_offset);
                            while(current_delta_offset> 0){
                                if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                                    uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_relaxed);
                                    uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                            std::memory_order_acquire);

                                    if (original_ts <= read_ts && (current_invalidation_ts == 0 ||
                                                                   current_invalidation_ts >
                                                                   read_ts)) {
                                        auto dst = current_delta->toID;
                                        int64_t curr_val = distances[dst-1];
                                        if (curr_val < 0 && gapbs::compare_and_swap(distances[dst-1], curr_val, distance)) {
                                            lqueue.push_back(dst);
                                            scout_count += -curr_val;
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
                        }
                    }
                }
                lqueue.flush();
                txn.thread_on_openmp_section_finish(thread_id);
            }
            graph->on_openmp_parallel_session_finish();
            return scout_count;
        }
        int64_t do_bfs_BUStep(SharedROTransaction&txn, int64_t distance, gapbs::Bitmap &front, gapbs::Bitmap &next){
            int64_t awake_count = 0;
            next.reset();
#pragma omp parallel reduction(+ : awake_count)
            {
                uint8_t thread_id = graph->get_openmp_worker_thread_id();
#pragma omp for schedule(dynamic, 1024)
                for (uint64_t u = 1; u <= max_vertex_id; u++) {
                    if (distances[u - 1] == std::numeric_limits<int64_t>::max()) continue; // the vertex does not exist
                    if (distances[u - 1] < 0) { // the node has not been visited yet
                        uint32_t current_delta_offset = 0;
                        auto current_block = txn.get_block_header(u, 1, thread_id, &current_delta_offset);
                        //determine which block to read
                        if(current_block->get_creation_time()<=read_ts)[[likely]]{//read latest block
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
                                    if (txn_tables->get_status(original_ts, status))[[likely]] {
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
                                                    txn_tables->reduce_op_count(original_ts,1);
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
                                if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                                    uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                            std::memory_order_acquire);
                                    //found the visible edge
                                    if(original_ts<=read_ts&&(current_invalidation_ts==0||current_invalidation_ts>read_ts)){
                                        //auto dst = current_delta->toID;
                                        if (front.get_bit(current_delta->toID - 1)) {
                                            distances[u - 1] = distance; // on each BUStep, all nodes will have the same distance
                                            awake_count++;
                                            next.set_bit(u - 1);
                                            break;
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
                            txn.unregister_thread_block_access(thread_id);
                        }else{//read previous block
                            txn.unregister_thread_block_access(thread_id);
                            bool found = false;
                            while (current_block->get_previous_ptr()) {
                                current_block = graph->get_block_manager().convert<EdgeDeltaBlockHeader>(
                                        current_block->get_previous_ptr());
                                if (read_ts >= current_block->get_creation_time())[[likely]] {
                                    found = true;
                                    break;
                                }
                            }
                            if(found)[[likely]]{
                                auto previous_block_offset = current_block->get_current_offset();
                                current_delta_offset = static_cast<uint32_t>(previous_block_offset & SIZE2MASK);
                                auto current_delta = current_block->get_edge_delta(current_delta_offset);
                                while(current_delta_offset> 0){
                                    if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                                        uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_relaxed);
                                        uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                                std::memory_order_acquire);

                                        if (original_ts <= read_ts && (current_invalidation_ts == 0 ||
                                                                       current_invalidation_ts >
                                                                       read_ts)) {
                                            if (front.get_bit(current_delta->toID - 1)) {
                                                distances[u - 1] = distance; // on each BUStep, all nodes will have the same distance
                                                awake_count++;
                                                next.set_bit(u - 1);
                                                break;
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
                            }
                        }

                    }
                }
                txn.thread_on_openmp_section_finish(thread_id);
            }
            graph->on_openmp_parallel_session_finish();
            return awake_count;
        }
        void do_bfs_QueueToBitmap(const gapbs::SlidingQueue<int64_t> &queue, gapbs::Bitmap &bm) {
#pragma omp parallel for
            for (auto q_iter = queue.begin(); q_iter < queue.end(); q_iter++) {
                int64_t u = *q_iter;
                bm.set_bit_atomic(u-1);
            }
        }
        void do_bfs_BitmapToQueue(const gapbs::Bitmap &bm, gapbs::SlidingQueue<int64_t> &queue){
#pragma omp parallel
            {
                gapbs::QueueBuffer<int64_t> lqueue(queue);
#pragma omp for
                for (uint64_t n=1; n <= max_vertex_id; n++)
                    if (bm.get_bit(n-1))
                        lqueue.push_back(n);
                lqueue.flush();
            }
            queue.slide_window();
        }

        void bfs(uint64_t root,int alpha = 15, int beta = 18){
            //std::cout<<"bfs running"<<std::endl;
            SharedROTransaction txn = graph->begin_shared_ro_transaction();
            read_ts = txn.get_read_ts();
            if(max_vertex_id!=graph->get_max_allocated_vid())[[unlikely]]{
                max_vertex_id = graph->get_max_allocated_vid();
                distances.resize(max_vertex_id);
            }
            gapbs::SlidingQueue<int64_t> queue(max_vertex_id);
            queue.push_back(root);
            queue.slide_window();
            gapbs::Bitmap curr(max_vertex_id);
            curr.reset();
            gapbs::Bitmap front(max_vertex_id);
            front.reset();
            int64_t edges_to_check = init_distance(txn);

            int64_t scout_count = 0;
            // retrieve the out degree of the root
            uint8_t thread_id = graph->get_openmp_worker_thread_id();
            scout_count = txn.get_neighborhood_size_signed(root,1,thread_id);
            graph->on_openmp_parallel_session_finish();
            int64_t distance = 1; // current distance
            distances[root-1] = 0;
            while (!queue.empty()) {
                if (scout_count > edges_to_check / alpha) {
                    int64_t awake_count, old_awake_count;
                    do_bfs_QueueToBitmap(queue,front);
                    awake_count = queue.size();
                    queue.slide_window();
                    do{
                        old_awake_count = awake_count;
                        awake_count = do_bfs_BUStep(txn,distance,front, curr);
                        front.swap(curr);
                        distance++;
                    }while((awake_count >= old_awake_count) || (awake_count > (int64_t) num_vertices / beta));
                    do_bfs_BitmapToQueue(front,queue);
                    scout_count = 1;
                }else{
                    edges_to_check -= scout_count;
                    scout_count = do_bfs_TDStep(txn,distance,queue);
                    queue.slide_window();
                    distance++;
                }
            }
            result.resize(max_vertex_id);
#pragma omp parallel
            {
                uint8_t thread_id = graph->get_openmp_worker_thread_id();
#pragma omp for
                for(uint64_t logical_id = 1; logical_id <= max_vertex_id; logical_id++){
                    std::string_view payload = txn.get_vertex(logical_id,thread_id);//they store external vid in the vertex data for experiments
                    if(payload.empty())[[unlikely]]{ // the vertex does not exist
                        result[logical_id-1] = std::make_pair(std::numeric_limits<uint64_t>::max(), std::numeric_limits<int64_t>::max());
                    } else {
                        /*if(*(reinterpret_cast<const uint64_t*>(payload.data()))==8196461){
                            std::cout<<" max vid is "<<max_vid<<" iteration is "<< logical_id<<std::endl;
                            std::cout<< *(reinterpret_cast<const uint64_t*>(payload.data())) <<" "<< scores[logical_id-1]<< std::endl;
                        }*/
                        result[logical_id-1] = std::make_pair(*(reinterpret_cast<const uint64_t*>(payload.data())), distances[logical_id-1]);
                    }
                }
                txn.thread_on_openmp_section_finish(thread_id);
            }
            graph->on_openmp_parallel_session_finish();
            txn.commit();
        }
        inline std::vector<int64_t>* get_raw_result(){return &distances;}
        inline std::vector<std::pair<uint64_t,int64_t>>* get_result(){return &result;}
    private:
        BwGraph* graph;
        uint64_t num_vertices;
        std::vector<int64_t> distances;
        std::vector<std::pair<uint64_t,int64_t>>result;
        uint64_t max_vertex_id;
        timestamp_t read_ts;
        TxnTables * txn_tables;
    };

    class PageRank{
    public:
        PageRank(BwGraph* input_graph, uint64_t input_num):graph(input_graph),num_vertices(input_num){

            txn_tables = &graph->get_txn_tables();
            max_vid = graph->get_max_allocated_vid();
            scores.resize(max_vid);
            result.resize(max_vid);
        }
        void compute_pagerank(uint64_t num_iterations, double damping_factor){
            SharedROTransaction txn = graph->begin_shared_ro_transaction();
            auto read_ts = txn.get_read_ts();
            const double init_score = 1.0/num_vertices;
            const double base_score = (1.0-damping_factor)/num_vertices;
            if(max_vid != graph->get_max_allocated_vid())[[unlikely]]{
                max_vid = graph->get_max_allocated_vid();
                //std::vector<double> scores(max_vid);
                scores.resize(max_vid);
                result.resize(max_vid);
            }
            std::vector<uint64_t> degrees(max_vid);
#pragma omp parallel
            {
                uint8_t thread_id = graph->get_openmp_worker_thread_id();
#pragma omp for
                for(uint64_t v = 1; v<=max_vid; v++){
                    scores[v-1] = init_score;
                    degrees[v-1]= txn.get_neighborhood_size(v,1,thread_id);
                }
                txn.thread_on_openmp_section_finish(thread_id);
            }
            //std::cout<<"neighborhood scanned"<<std::endl;
            graph->on_openmp_parallel_session_finish();
            gapbs::pvector<double> outgoing_contrib(max_vid, 0.0);
            for(uint64_t iteration = 0; iteration < num_iterations; iteration++){
                double dangling_sum = 0.0;
#pragma omp parallel for reduction(+:dangling_sum)
                for(uint64_t v = 1; v <= max_vid; v++){
                    uint64_t out_degree = degrees[v-1];
                    if(out_degree == std::numeric_limits<uint64_t>::max()){
                        continue; // the vertex does not exist
                    } else if (out_degree == 0){ // this is a sink
                        dangling_sum += scores[v-1];
                    } else {
                        outgoing_contrib[v-1] = scores[v-1] / out_degree;
                    }
                }
                dangling_sum /= num_vertices;
#pragma omp parallel
                {
                    uint8_t thread_id = graph->get_openmp_worker_thread_id();
#pragma omp for schedule(dynamic, 64)
                    for (uint64_t v = 1; v <= max_vid; v++) {
                        if (degrees[v-1] == std::numeric_limits<uint64_t>::max())[[unlikely]] { continue; }
                        double incoming_total = 0;
                        uint32_t current_delta_offset = 0;
                        auto current_block = txn.get_block_header(v,1,thread_id,&current_delta_offset);
                        //determine which block to read
                        if(current_block->get_creation_time()<=read_ts)[[likely]]{//read latest block
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
                                    if (txn_tables->get_status(original_ts, status))[[likely]] {
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
                                                    txn_tables->reduce_op_count(original_ts,1);
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
                                if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                                    uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                            std::memory_order_acquire);
                                    //found the visible edge
                                    if(original_ts<=read_ts&&(current_invalidation_ts==0||current_invalidation_ts>read_ts)){
                                        incoming_total += outgoing_contrib[current_delta->toID-1];
                                    }
                                }
#if USING_READER_PREFETCH
                                //if(current_delta_offset>=prefetch_offset)
                                _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                                current_delta_offset -= ENTRY_DELTA_SIZE;
                                current_delta++;
                            }
                            txn.unregister_thread_block_access(thread_id);
                        }else{//read previous block
                            txn.unregister_thread_block_access(thread_id);
                            bool found = false;
                            while (current_block->get_previous_ptr()) {
                                current_block = graph->get_block_manager().convert<EdgeDeltaBlockHeader>(
                                        current_block->get_previous_ptr());
                                if (read_ts >= current_block->get_creation_time())[[likely]] {
                                    found = true;
                                    break;
                                }
                            }
                            if(found)[[likely]]{
                                auto previous_block_offset = current_block->get_current_offset();
                                current_delta_offset = static_cast<uint32_t>(previous_block_offset & SIZE2MASK);
                                auto current_delta = current_block->get_edge_delta(current_delta_offset);
                                while(current_delta_offset> 0){
                                    if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                                        uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_relaxed);
                                        uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                                std::memory_order_acquire);

                                        if (original_ts <= read_ts && (current_invalidation_ts == 0 ||
                                                                       current_invalidation_ts >
                                                                       read_ts)) {
                                            incoming_total += outgoing_contrib[current_delta->toID-1];
                                        }
                                    }
#if USING_READER_PREFETCH
                                    //if(current_delta_offset>=prefetch_offset)
                                    _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                                    current_delta_offset -= ENTRY_DELTA_SIZE;
                                    current_delta++;
                                }
                            }
                        }
                        scores[v-1] = base_score + damping_factor * (incoming_total + dangling_sum);

                    }
                    txn.thread_on_openmp_section_finish(thread_id);
                }
                graph->on_openmp_parallel_session_finish();
            }
#pragma omp parallel
            {
                uint8_t thread_id = graph->get_openmp_worker_thread_id();
#pragma omp for
                for(uint64_t logical_id = 1; logical_id <= max_vid; logical_id++){
                    std::string_view payload = txn.get_vertex(logical_id,thread_id);//they store external vid in the vertex data for experiments
                    if(payload.empty())[[unlikely]]{ // the vertex does not exist
                        result[logical_id-1] = std::make_pair(std::numeric_limits<uint64_t>::max(), std::numeric_limits<double>::max());
                    } else {
                        /*if(*(reinterpret_cast<const uint64_t*>(payload.data()))==8196461){
                            std::cout<<" max vid is "<<max_vid<<" iteration is "<< logical_id<<std::endl;
                            std::cout<< *(reinterpret_cast<const uint64_t*>(payload.data())) <<" "<< scores[logical_id-1]<< std::endl;
                        }*/
                        result[logical_id-1] = std::make_pair(*(reinterpret_cast<const uint64_t*>(payload.data())), scores[logical_id-1]);
                    }
                }
                txn.thread_on_openmp_section_finish(thread_id);
            }
            graph->on_openmp_parallel_session_finish();
            txn.commit();
        }

        inline std::vector<double>* get_raw_result(){
            return &scores;
        }
        inline std::vector<std::pair<uint64_t, double>>* get_result(){
            return &result;
        }
    private:
        BwGraph* graph;
        std::vector<double> scores;
        uint64_t num_vertices;
        std::vector<std::pair<uint64_t, double>>result;
        TxnTables* txn_tables;
        uint64_t max_vid;
    };

    class SSSP{
    public:
        SSSP(BwGraph* input_graph, uint64_t max_vid):graph(input_graph),max_vid(max_vid),dist(max_vid, std::numeric_limits<double>::infinity()){
            result.resize(max_vid);
            txn_tables = &graph->get_txn_tables();
        }
        inline void reset(){
            std::fill(dist.begin(), dist.end(), std::numeric_limits<double>::infinity());
        }
        void compute(uint64_t source, double delta){
            //std::cout<<"entering compute algorithm"<<std::endl;
            auto txn = graph->begin_shared_ro_transaction();
            auto read_ts =txn.get_read_ts();
            //std::cout<<"using dynamic sssp"<<std::endl;
            /*if(graph->get_max_allocated_vid()!=max_vid)[[unlikely]]{
                max_vid = graph->get_max_allocated_vid();
                dist.resize(max_vid);
                std::fill(dist.begin(),dist.end(),std::numeric_limits<double>::infinity());
                result.resize(max_vid);
            }*/
            dist[source-1] = 0;
            uint64_t num_edges;

            num_edges = txn.get_total_edge_num(1);
            gapbs::pvector<uint64_t> frontier(num_edges);
            // two element arrays for double buffering curr=iter&1, next=(iter+1)&1
            size_t shared_indexes[2] = {0, kMaxBin};
            size_t frontier_tails[2] = {1, 0};
            frontier[0] = source;
            using NodeID = uint64_t;
            using WeightT = double;
#pragma omp parallel
            {
                uint8_t thread_id = graph->get_openmp_worker_thread_id();
                std::vector<std::vector<uint64_t> > local_bins(0);
                size_t iter = 0;
                while (shared_indexes[iter&1] != kMaxBin) {
                    size_t &curr_bin_index = shared_indexes[iter&1];
                    size_t &next_bin_index = shared_indexes[(iter+1)&1];
                    size_t &curr_frontier_tail = frontier_tails[iter&1];
                    size_t &next_frontier_tail = frontier_tails[(iter+1)&1];
                    //std::cout<<"current frontier tail is "<<curr_frontier_tail<<std::endl;
#pragma omp for nowait schedule(dynamic, 64)
                    for (size_t i=0; i < curr_frontier_tail; i++) {
                        //std::cout<<"current i is "<<std::endl;
                        NodeID u = frontier[i];
                        if (dist[u-1] >= delta * static_cast<WeightT>(curr_bin_index)) {
                            //get block
                            uint32_t current_delta_offset = 0;
                            auto current_block = txn.get_block_header(u,1,thread_id,&current_delta_offset);
                            //determine which block to read
                            if(current_block->get_creation_time()<=read_ts)[[likely]]{//read latest block
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
                                        if (txn_tables->get_status(original_ts, status))[[likely]] {
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
                                                        txn_tables->reduce_op_count(original_ts,1);
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
                                    if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                                        uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                                std::memory_order_acquire);
                                        //found the visible edge
                                        if(original_ts<=read_ts&&(current_invalidation_ts==0||current_invalidation_ts>read_ts)){
                                            auto v = current_delta->toID;
                                            double w = *reinterpret_cast<double*>(current_delta->data);
                                            WeightT old_dist = dist[v-1];
                                            WeightT new_dist = dist[u-1] + w;
                                            if (new_dist < old_dist) {
                                                bool changed_dist = true;
                                                while (!gapbs::compare_and_swap(dist[v-1], old_dist, new_dist)) {
                                                    old_dist = dist[v-1];
                                                    if (old_dist <= new_dist) {
                                                        changed_dist = false;
                                                        break;
                                                    }
                                                }
                                                if (changed_dist) {
                                                    size_t dest_bin = new_dist/delta;
                                                    if (dest_bin >= local_bins.size()) {
                                                        local_bins.resize(dest_bin+1);
                                                    }
                                                    local_bins[dest_bin].push_back(v);
                                                }
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
                                txn.unregister_thread_block_access(thread_id);
                            }else{//read previous block
                                txn.unregister_thread_block_access(thread_id);
                                bool found = false;
                                while (current_block->get_previous_ptr()) {
                                    current_block = graph->get_block_manager().convert<EdgeDeltaBlockHeader>(
                                            current_block->get_previous_ptr());
                                    if (read_ts >= current_block->get_creation_time())[[likely]] {
                                        found = true;
                                        break;
                                    }
                                }
                                if(found)[[likely]]{
                                    auto previous_block_offset = current_block->get_current_offset();
                                    current_delta_offset = static_cast<uint32_t>(previous_block_offset & SIZE2MASK);
                                    auto current_delta = current_block->get_edge_delta(current_delta_offset);
                                    while(current_delta_offset> 0){
                                        if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                                            uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_relaxed);
                                            uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                                    std::memory_order_acquire);

                                            if (original_ts <= read_ts && (current_invalidation_ts == 0 ||
                                                                           current_invalidation_ts >
                                                                           read_ts))
                                            {
                                                auto v = current_delta->toID;
                                                double w = *reinterpret_cast<double*>(current_delta->data);
                                                WeightT old_dist = dist[v-1];
                                                WeightT new_dist = dist[u-1] + w;
                                                if (new_dist < old_dist) {
                                                    bool changed_dist = true;
                                                    while (!gapbs::compare_and_swap(dist[v-1], old_dist, new_dist)) {
                                                        old_dist = dist[v-1];
                                                        if (old_dist <= new_dist) {
                                                            changed_dist = false;
                                                            break;
                                                        }
                                                    }
                                                    if (changed_dist) {
                                                        size_t dest_bin = new_dist/delta;
                                                        if (dest_bin >= local_bins.size()) {
                                                            local_bins.resize(dest_bin+1);
                                                        }
                                                        local_bins[dest_bin].push_back(v);
                                                    }
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
                                }
                            }
                        }
                    }

                    txn.thread_on_openmp_section_finish(thread_id);
                    //std::cout<<"out of omp for "<<(uint32_t)thread_id<<std::endl;
                    for (size_t i=curr_bin_index; i < local_bins.size(); i++) {
                        if (!local_bins[i].empty()) {
#pragma omp critical
                            next_bin_index = std::min(next_bin_index, i);
                            break;
                        }
                    }

#pragma omp barrier
#pragma omp single nowait
                    {
                        curr_bin_index = kMaxBin;
                        curr_frontier_tail = 0;
                    }

                    if (next_bin_index < local_bins.size()) {
                        size_t copy_start = gapbs::fetch_and_add(next_frontier_tail, local_bins[next_bin_index].size());
                        copy(local_bins[next_bin_index].begin(), local_bins[next_bin_index].end(), frontier.data() + copy_start);
                        local_bins[next_bin_index].resize(0);
                    }

                    iter++;
#pragma omp barrier
                }
            }
            graph->on_openmp_parallel_session_finish();
#pragma omp parallel
            {
                uint8_t thread_id = graph->get_openmp_worker_thread_id();
#pragma omp for
                for(uint64_t logical_id = 1; logical_id <= max_vid; logical_id++){
                    std::string_view payload = txn.get_vertex(logical_id,thread_id);//they store external vid in the vertex data for experiments
                    if(payload.empty())[[unlikely]]{ // the vertex does not exist
                        result[logical_id-1] = std::make_pair(std::numeric_limits<uint64_t>::max(), std::numeric_limits<double>::max());
                    } else {
                        result[logical_id-1] = std::make_pair(*(reinterpret_cast<const uint64_t*>(payload.data())), dist[logical_id-1]);
                    }
                }
                txn.thread_on_openmp_section_finish(thread_id);
            }
            graph->on_openmp_parallel_session_finish();
            txn.commit();
        }
        inline std::vector<std::pair<uint64_t, double>>* get_result(){return &result;}
    private:
        BwGraph* graph;
        uint64_t max_vid;
        gapbs::pvector<double> dist; //cost to reach each vertex
        std::vector<std::pair<uint64_t, double>>result;//final result
        size_t kMaxBin = std::numeric_limits<size_t>::max()/2;
        TxnTables * txn_tables;
    };

    class OneHopNeighbors{
    public:
        OneHopNeighbors(BwGraph* input_graph):graph(input_graph),txn_tables(&graph->get_txn_tables()){}
        void find_one_hop_neighbors(std::vector<uint64_t>&vertices){
            vertex_one_hop_neighbors.reserve(vertices.size());
            auto txn = graph->begin_shared_ro_transaction();
            auto read_ts =txn.get_read_ts();
            //initialize the entries
            for(auto source : vertices){
                vertex_one_hop_neighbors[source];
            }
#pragma omp parallel
            {
                uint8_t thread_id = graph->get_openmp_worker_thread_id();
#pragma omp for
                for(unsigned long source : vertices){
                    uint32_t current_delta_offset = 0;
                    auto current_block = txn.get_block_header(source,1,thread_id,&current_delta_offset);
                    if(!current_block)[[unlikely]]{
                        continue;
                    }

                    /*std::string_view payload = txn.get_vertex(source,thread_id);//they store external vid in the vertex data for experiments
                    vertex_t physical_source_id = *(reinterpret_cast<const uint64_t*>(payload.data()));
                    auto& neighbors = vertex_two_hop_neighbors[physical_source_id];*/
                    auto& neighbors = vertex_one_hop_neighbors[source];
                    //std::vector<uint64_t> hop_1_neighbors;
                    //determine which block to read
                    if(current_block->get_creation_time()<=read_ts)[[likely]]{//read latest block
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
                                if (txn_tables->get_status(original_ts, status))[[likely]] {
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
                                                txn_tables->reduce_op_count(original_ts,1);
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
                            if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                                uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                        std::memory_order_acquire);
                                //found the visible edge
                                if(original_ts<=read_ts&&(current_invalidation_ts==0||current_invalidation_ts>read_ts)){
                                    neighbors.emplace_back(current_delta->toID);
                                    //auto neighbor_payload = txn.get_vertex(current_delta->toID,thread_id);
                                    //neighbors.emplace_back(*(reinterpret_cast<const uint64_t*>(neighbor_payload.data())));
                                    //hop_1_neighbors.emplace_back(current_delta->toID);
                                }
                            }
#if USING_READER_PREFETCH
                            //if(current_delta_offset>=prefetch_offset)
                                _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                            current_delta_offset -= ENTRY_DELTA_SIZE;
                            current_delta++;
                        }
                        txn.unregister_thread_block_access(thread_id);
                    }else{//read previous block
                        txn.unregister_thread_block_access(thread_id);
                        bool found = false;
                        while (current_block->get_previous_ptr()) {
                            current_block = graph->get_block_manager().convert<EdgeDeltaBlockHeader>(
                                    current_block->get_previous_ptr());
                            if (read_ts >= current_block->get_creation_time())[[likely]] {
                                found = true;
                                break;
                            }
                        }
                        if(found)[[likely]]{
                            auto previous_block_offset = current_block->get_current_offset();
                            current_delta_offset = static_cast<uint32_t>(previous_block_offset & SIZE2MASK);
                            auto current_delta = current_block->get_edge_delta(current_delta_offset);
                            while(current_delta_offset> 0){
                                if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                                    uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_relaxed);
                                    uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                            std::memory_order_acquire);

                                    if (original_ts <= read_ts && (current_invalidation_ts == 0 ||
                                                                   current_invalidation_ts >
                                                                   read_ts)) {
                                        neighbors.emplace_back(current_delta->toID);
                                        //auto neighbor_payload = txn.get_vertex(current_delta->toID,thread_id);
                                        //neighbors.emplace_back(*(reinterpret_cast<const uint64_t*>(neighbor_payload.data())));
                                        //hop_1_neighbors.emplace_back(current_delta->toID);
                                    }
                                }
#if USING_READER_PREFETCH
                                //if(current_delta_offset>=prefetch_offset)
                                    _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                                current_delta_offset -= ENTRY_DELTA_SIZE;
                                current_delta++;
                            }
                        }
                    }
                }
                txn.thread_on_openmp_section_finish(thread_id);
            }
            graph->on_openmp_parallel_session_finish();
        }
        inline std::unordered_map<uint64_t, std::vector<uint64_t>>* get_result(){
            return &vertex_one_hop_neighbors;
        }

    private:
        BwGraph* graph;
        //uint64_t max_vid;
        TxnTables * txn_tables;
        //std::unordered_map<uint64_t,std::vector<uint64_t>>results;
        std::unordered_map<uint64_t, std::vector<uint64_t>>vertex_one_hop_neighbors;
    };

    /*
     * give a set of vertices, find their two hop neighbors
     */
    class TwoHopNeighbors{
    public:
        TwoHopNeighbors(BwGraph* input_graph):graph(input_graph),txn_tables(&graph->get_txn_tables()){}

        void find_two_hop_neighbors(std::vector<uint64_t>&vertices){
            //std::cout<<"find 2 neighbors run"<<std::endl;
            vertex_two_hop_neighbors.reserve(vertices.size());
            auto txn = graph->begin_shared_ro_transaction();
            auto read_ts =txn.get_read_ts();
            //initialize the entries
            for(auto source : vertices){
                vertex_two_hop_neighbors[source];
            }
#pragma omp parallel
            {
                uint8_t thread_id = graph->get_openmp_worker_thread_id();
#pragma omp for
                for(unsigned long source : vertices){
                    uint32_t current_delta_offset = 0;
                    auto current_block = txn.get_block_header(source,1,thread_id,&current_delta_offset);
                    if(!current_block)[[unlikely]]{
                        continue;
                    }

                    /*std::string_view payload = txn.get_vertex(source,thread_id);//they store external vid in the vertex data for experiments
                    vertex_t physical_source_id = *(reinterpret_cast<const uint64_t*>(payload.data()));
                    auto& neighbors = vertex_two_hop_neighbors[physical_source_id];*/
                    auto& neighbors = vertex_two_hop_neighbors[source];
                    std::vector<uint64_t> hop_1_neighbors;
                    //determine which block to read
                    if(current_block->get_creation_time()<=read_ts)[[likely]]{//read latest block
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
                                if (txn_tables->get_status(original_ts, status))[[likely]] {
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
                                                txn_tables->reduce_op_count(original_ts,1);
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
                            if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                                uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                        std::memory_order_acquire);
                                //found the visible edge
                                if(original_ts<=read_ts&&(current_invalidation_ts==0||current_invalidation_ts>read_ts)){
                                    neighbors.emplace_back(current_delta->toID);
                                    //auto neighbor_payload = txn.get_vertex(current_delta->toID,thread_id);
                                    //neighbors.emplace_back(*(reinterpret_cast<const uint64_t*>(neighbor_payload.data())));
                                    hop_1_neighbors.emplace_back(current_delta->toID);
                                }
                            }
#if USING_READER_PREFETCH
                            //if(current_delta_offset>=prefetch_offset)
                                _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                            current_delta_offset -= ENTRY_DELTA_SIZE;
                            current_delta++;
                        }
                        txn.unregister_thread_block_access(thread_id);
                    }else{//read previous block
                        txn.unregister_thread_block_access(thread_id);
                        bool found = false;
                        while (current_block->get_previous_ptr()) {
                            current_block = graph->get_block_manager().convert<EdgeDeltaBlockHeader>(
                                    current_block->get_previous_ptr());
                            if (read_ts >= current_block->get_creation_time())[[likely]] {
                                found = true;
                                break;
                            }
                        }
                        if(found)[[likely]]{
                            auto previous_block_offset = current_block->get_current_offset();
                            current_delta_offset = static_cast<uint32_t>(previous_block_offset & SIZE2MASK);
                            auto current_delta = current_block->get_edge_delta(current_delta_offset);
                            while(current_delta_offset> 0){
                                if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                                    uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_relaxed);
                                    uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                            std::memory_order_acquire);

                                    if (original_ts <= read_ts && (current_invalidation_ts == 0 ||
                                                                   current_invalidation_ts >
                                                                   read_ts)) {
                                        neighbors.emplace_back(current_delta->toID);
                                        //auto neighbor_payload = txn.get_vertex(current_delta->toID,thread_id);
                                        //neighbors.emplace_back(*(reinterpret_cast<const uint64_t*>(neighbor_payload.data())));
                                        hop_1_neighbors.emplace_back(current_delta->toID);
                                    }
                                }
#if USING_READER_PREFETCH
                                //if(current_delta_offset>=prefetch_offset)
                                    _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                                current_delta_offset -= ENTRY_DELTA_SIZE;
                                current_delta++;
                            }
                        }
                    }
                    //now check the hop 1 neighbors' neighbors
                    for(auto hop_1_source : hop_1_neighbors){
                        current_block = txn.get_block_header(hop_1_source,1,thread_id,&current_delta_offset);
                        if(current_block->get_creation_time()<=read_ts)[[likely]]{//read latest block
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
                                    if (txn_tables->get_status(original_ts, status))[[likely]] {
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
                                                    txn_tables->reduce_op_count(original_ts,1);
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
                                if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                                    uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                            std::memory_order_acquire);
                                    //found the visible edge
                                    if(original_ts<=read_ts&&(current_invalidation_ts==0||current_invalidation_ts>read_ts)){
                                        if(current_delta->toID!=source)[[likely]]{
                                            neighbors.emplace_back(current_delta->toID);
                                            //auto neighbor_payload = txn.get_vertex(current_delta->toID,thread_id);
                                            //neighbors.emplace_back(*(reinterpret_cast<const uint64_t*>(neighbor_payload.data())));
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
                            txn.unregister_thread_block_access(thread_id);
                        }else{//read previous block
                            txn.unregister_thread_block_access(thread_id);
                            bool found = false;
                            while (current_block->get_previous_ptr()) {
                                current_block = graph->get_block_manager().convert<EdgeDeltaBlockHeader>(
                                        current_block->get_previous_ptr());
                                if (read_ts >= current_block->get_creation_time())[[likely]] {
                                    found = true;
                                    break;
                                }
                            }
                            if(found)[[likely]]{
                                auto previous_block_offset = current_block->get_current_offset();
                                current_delta_offset = static_cast<uint32_t>(previous_block_offset & SIZE2MASK);
                                auto current_delta = current_block->get_edge_delta(current_delta_offset);
                                while(current_delta_offset> 0){
                                    if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                                        uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_relaxed);
                                        uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                                std::memory_order_acquire);

                                        if (original_ts <= read_ts && (current_invalidation_ts == 0 ||
                                                                       current_invalidation_ts >
                                                                       read_ts)) {
                                            if(current_delta->toID!=source)[[likely]]{
                                                neighbors.emplace_back(current_delta->toID);
                                                //auto neighbor_payload = txn.get_vertex(current_delta->toID,thread_id);
                                                //neighbors.emplace_back(*(reinterpret_cast<const uint64_t*>(neighbor_payload.data())));
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
                            }
                        }
                    }
                }//end of all vertex loop
                txn.thread_on_openmp_section_finish(thread_id);
            }//end of omp session
            graph->on_openmp_parallel_session_finish();
            //already translated during execution
        }

        inline std::unordered_map<uint64_t, std::vector<uint64_t>>* get_result(){
            return &vertex_two_hop_neighbors;
        }

    private:
        BwGraph* graph;
        //uint64_t max_vid;
        TxnTables * txn_tables;
        //std::unordered_map<uint64_t,std::vector<uint64_t>>results;
        std::unordered_map<uint64_t, std::vector<uint64_t>>vertex_two_hop_neighbors;
    };
    //todo: class AlgorithmHandler
}

