cmake_minimum_required(VERSION 3.25)
project(BwGraph_v2)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake)
set(CMAKE_CXX_FLAGS_DEBUG "-ltbb -std=c++17 -g -fno-omit-frame-pointer -Wall -Wextra -Wnon-virtual-dtor -pedantic -Wconversion -Wlogical-op -fsanitize=address,undefined")
find_package(TBB REQUIRED PATHS /tmp/my_installed_onetbb/lib/cmake/TBB)
add_library(parallel_hashmap_lib STATIC Libraries/parallel_hashmap/something.cpp)
target_include_directories(parallel_hashmap_lib PUBLIC parallel_hashmap)
add_library(bwgraph_lib STATIC core/block.cpp core/transaction_tables.cpp core/bw_index.cpp  core/bwgraph.cpp core/commit_manager.cpp src/bw_transaction.cpp)
target_include_directories(bwgraph_lib PUBLIC public core)
add_executable(BwGraph_v2 main.cpp ) #core/types.hpp core/block_manager.hpp core/utils.hpp core/block.hpp core/exceptions.hpp core/transaction_tables.cpp core/transaction_tables.hpp core/graph_global.hpp Libraries/parallel_hashmap/something.cpp
#target_link_directories(BwGraph_v2 PUBLIC core/ )
target_link_libraries(BwGraph_v2 TBB::tbb parallel_hashmap_lib bwgraph_lib)