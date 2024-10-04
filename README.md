# GTX

## Description
This is GTX, a main memory graph system that manages and queries dynamic graphs. GTX supports concurrent read-write and read-only transactions with snapshot isolation. GTX supports graph analytics using its OpenMP-tailored read-only transactions and transaction adjacency list scan protocol. It has been evaluated against state-of-the-art transactional graphy systems using [GFE_Driver](https://github.com/Jiboxiake/gfe_driver_gtx). Currently we only show the anonymized version but we aim to publish the final experiment framework with full dataset when the paper decision is finalized.
## Build
### Prerequisites 
- We only tested it on Linux.
- [CMake](https://gitlab.kitware.com/cmake/cmake)
- [TBB](https://github.com/oneapi-src/oneTBB) 
- C++20 is preferred although it can run with C++17 

### Build instructions 
```
- mkdir build && cd build
- cmake -DCMAKE_BUILD_TYPE=Release ..
- make -j
```
## Usage
### Include GTX in your own project
- include library GTX in your project's CMakeLists.txt. 
- copy bind/GTX.hpp into your project. 
- include GTX.hpp to use GTX
### API
The full GTX APIs can be found in /bind/GTX.hpp.
Here we list the core APIs to manage and query a dynamic graph using GTX

#### GTX
- public GTX() 
- ROTransaction begin_read_only_transaction()
- SharedROTransaction begin_shared_read_only_transaction()
- RWTransaction begin_read_write_transaction()
- vertex_t get_max_allocated_vid()
- void set_worker_thread_num(uint64_t new_size)
- void set_writer_thread_num(uint64_t writer_thread_num)
- void whole_label_graph_eager_consolidation(label_t label)
- EdgeDeltaBlockHeader* get_edge_block(vertex_t vid, label_t l)
- uint8_t get_openmp_worker_thread_id()
- PageRankHandler get_pagerank_handler(uint64_t num);
- BFSHandler get_bfs_handler(uint64_t num);
- SSSPHandler get_sssp_handler(uint64_t num);
- OneHopNeighborsHandler get_one_hop_neighbors_handler();
- TwoHopNeighborsHandler get_two_hop_neighbors_handler();

#### ROTransaction
- std::string_view get_vertex(vertex_t src)
- std::string_view get_edge(vertex_t src, vertex_t dst, label_t label)
- double get_edge_weight(vertex_t src, vertex_t dst, label_t label)
- SimpleEdgeDeltaIterator simple_get_edges(vertex_t src, label_t label)

#### SharedROTransaction
This class is used by OpenMP worker thread to implement graph analytics.
- std::string_view get_vertex(vertex_t src,uint8_t thread_id)
- std::string_view get_edge(vertex_t src, vertex_t dst, label_t label,uint8_t thread_id)     
- SimpleEdgeDeltaIterator simple_get_edges(vertex_t src, label_t label,uint8_t thread_id)
- void simple_get_edges(vertex_t src, label_t label, uint8_t thread_id, SimpleEdgeDeltaIterator& edge_iterator)
- SimpleEdgeDeltaIterator generate_edge_delta_iterator(uint8_t thread_id)
- StaticEdgeDeltaIterator generate_static_edge_delta_iterator()
- StaticEdgeDeltaIterator static_get_edges(vertex_t src, label_t label)
- void static_get_edges(vertex_t src, label_t label, StaticEdgeDeltaIterator& edge_iterator)

#### RWTransaction
- std::string_view get_vertex(vertex_t src)
- std::string_view get_edge(vertex_t src, vertex_t dst, label_t label)
- SimpleEdgeDeltaIterator simple_get_edges(vertex_t src, label_t label)
- vertex_t new_vertex()
- void put_vertex(vertex_t vertex_id, std::string_view data)
- bool checked_put_edge(vertex_t src, label_t label, vertex_t dst, std::string_view edge_data)
- bool checked_delete_edge(vertex_t src, label_t label, vertex_t dst)

#### SimpleEdgeDeltaIterator
- bool valid()
- void close()
- void next()
- vertex_t dst_id() 
- std::string_view  edge_delta_data() 
- uint64_t get_vertex_degree()

#### StaticEdgeDeltaIterator
The static iterator is used to scan a static graph after the graph is loaded.
- void clear()
- bool valid()
- void next()
- uint32_t vertex_degree()
- vertex_t dst_id() 
- std::string_view  edge_delta_data()

#### BFSHandlers
- void compute(uint64_t root,int alpha = 15, int beta = 18);

#### PageRankHandler
- void compute(uint64_t num_iterations, double damping_factor);

#### SSSPHandler
- void compute(uint64_t source, double delta);

#### OneHopNeighborsHandler
- void compute(std::vector<uint64_t>&vertices);

#### TwoHopNeighborsHandler
- void compute(std::vector<uint64_t>&vertices);

### Example
```
cmake_minimum_required(VERSION 3.24)
project(GTX_Link_Test)
set(CMAKE_CXX_STANDARD 20)
list(APPEND CMAKE_MODULE_PATH "~/cmake_modules/FindTBB")
set(CMAKE_CXX_FLAGS_DEBUG "-std=c++20 -g -fno-omit-frame-pointer -Wall -Wextra -Wnon-virtual-dtor -pedantic -Wconversion -Wlogical-op")
set(CMAKE_CXX_FLAGS_RELEASE "-std=c++20 -g -fno-omit-frame-pointer -Wall -Wextra -Wnon-virtual-dtor -pedantic -Wconversion -Wlogical-op -O3 -DNDEBUG")
LINK_DIRECTORIES(/home/zhou822/GTX_Mono_Simple_Iterator/build/)
add_executable(GTX_Link_Test main.cpp)
TARGET_LINK_LIBRARIES(GTX_Link_Test GTX)
```

```
#include <iostream>
#include "GTX.hpp"
using GTX = gt::Graph;
int main() {
    GTX g;

    //create a read-write transaction
    auto txn = g.begin_read_write_transaction();

    std::string data = "abc";

    //create a new vertex, return its internal vertex id
    auto vid1 = txn.new_vertex();
    auto vid2 = txn.new_vertex();

    //create a new version of the vertex the txn just created
    txn.put_vertex(vid1, data.c_str());
    txn.put_vertex(vid2, data.c_str());

    //create/update a new version of the edge of label 1 and data.
    txn.checked_put_edge(vid1,1,vid2,data.c_str());
    txn.checked_put_edge(vid2,1,vid1,data.c_str());
    txn.commit();

    //create a read-only transaction
    auto r_txn = g.begin_read_only_transaction();

    //single edge read
    auto result = r_txn.get_edge(vid1,vid2,1);
    if(result.at(0)!='a'||result.at(1)!='b'||result.at(2)!='c'){
        std::cout<<"error"<<std::endl;
    }else{
        std::cout<<"no error"<<std::endl;
    }

    //create an iterator to scan the adjacency list of a vertex
    auto iterator = r_txn.simple_get_edges(vid1,1);

    //scan the adjacency list
    while(iterator.valid()){
        auto v = iterator.dst_id();
        if(v!=vid2){
            std::cout<<"error"<<std::endl;
        }
        auto result2 = iterator.edge_delta_data();
        if(result2.at(0)!='a'||result2.at(1)!='b'||result2.at(2)!='c'){
            std::cout<<"error"<<std::endl;
        }else{
            std::cout<<"no error"<<std::endl;
        }
    }
    r_txn.commit();

    //do analytics:
    vertex_t root =8;//start algorithm from vertex with id =8
    int32_t num_iterations = 10;
    double damping_factor = 0.5;
    auto bfs_handler =  g.get_bfs_handler(num_vertices);
    bfs_handler.compute(root);
    auto pr_handler = g.get_pagerank_handler(num_vertices);
    pr_handlercompute(num_iterations,damping_factor);
    auto handler = g.get_sssp_handler(max_vertex_id);
    double delta = 2.0;
    handler.compute(root,delta);
    return 0;
}
```