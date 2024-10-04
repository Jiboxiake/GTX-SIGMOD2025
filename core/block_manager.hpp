//
// Created by zhou822 on 5/22/23.
//

//#ifndef BWGRAPH_V2_BLOCK_MANAGER_HPP
//#define BWGRAPH_V2_BLOCK_MANAGER_HPP
#pragma once
//adopted from https://github.com/thu-pacman/LiveGraph
#include <atomic>
#include <cstdlib>
#include <mutex>
#include <stdexcept>
#include <vector>

#include <tbb/enumerable_thread_specific.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include "types.hpp"
#include "graph_global.hpp"
namespace GTX
{
    class BlockManager
    {
    public:
        constexpr static uintptr_t NULLPOINTER = 0; // UINTPTR_MAX;

        BlockManager(std::string path, size_t _capacity = 1ul << 30)
                : capacity(_capacity),
                  mutex(),
                  free_blocks(std::vector<std::vector<uintptr_t>>(LARGE_BLOCK_THRESHOLD, std::vector<uintptr_t>())),
                  large_free_blocks(MAX_ORDER, std::vector<uintptr_t>())
#if TRACK_GARBAGE_RECORD_TIME
                  ,thread_local_garbage_record_time(0),
                  thread_local_allocation_time(0),
                  thread_local_free_time(0)
#endif
        {
            if (path.empty())
            {
                fd = EMPTY_FD;
                data =
                        mmap(nullptr, capacity, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
                if (data == MAP_FAILED){
                    printf(" Value of errno: %d\n ", errno);
                    throw std::runtime_error("mmap block error.");
                }
            }
            else
            {
                fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0640);
                if (fd == EMPTY_FD){
                    printf(" Value of errno: %d\n ", errno);
                    throw std::runtime_error("open block file error.");
                }
                if (ftruncate(fd, FILE_TRUNC_SIZE) != 0){
                    printf(" Value of errno: %d\n ", errno);
                    throw std::runtime_error("ftruncate block file error.");
                }
                data = mmap(nullptr, capacity, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
                if (data == MAP_FAILED)
                {
                    printf(" Value of errno: %d\n ", errno);
                    throw std::runtime_error("mmap block error.");
                }
            }

            if (madvise(data, capacity, MADV_RANDOM) != 0){
                printf(" Value of errno: %d\n ", errno);
                throw std::runtime_error("madvise block error.");
            }

            file_size = FILE_TRUNC_SIZE;
            used_size = 0;

            null_holder = alloc(LARGE_BLOCK_THRESHOLD);
        }

        ~BlockManager()
        {
            free(null_holder, LARGE_BLOCK_THRESHOLD);
            msync(data, capacity, MS_SYNC);
            munmap(data, capacity);
            if (fd != EMPTY_FD)
                close(fd);
        }

        uintptr_t alloc(order_t order)
        {
#if TRACK_GARBAGE_RECORD_TIME
            auto start = std::chrono::high_resolution_clock::now();
#endif
            if(order>MAX_ORDER){
                throw std::runtime_error("error, too large block");
            }
            uintptr_t pointer = NULLPOINTER;
            if (order < LARGE_BLOCK_THRESHOLD)
            {
                pointer = pop(free_blocks.local(), order);
            }
            else
            {
                std::lock_guard<std::mutex> lock(mutex);
                pointer = pop(large_free_blocks, order);
            }

            if (pointer == NULLPOINTER)
            {
                size_t block_size = 1ul << order;
                pointer = used_size.fetch_add(block_size);

                if (pointer + block_size >= file_size)
                {
                    auto new_file_size = ((pointer + block_size) / FILE_TRUNC_SIZE + 1) * FILE_TRUNC_SIZE;
                    std::lock_guard<std::mutex> lock(mutex);
                    if (new_file_size >= file_size)
                    {
                        if (fd != EMPTY_FD)
                        {
                            if (ftruncate(fd, new_file_size) != 0){
                                printf(" Value of errno: %d\n ", errno);
                                throw std::runtime_error("ftruncate block file error.");
                            }
                        }
                        file_size = new_file_size;
                    }
                }
            }
#if TRACK_GARBAGE_RECORD_TIME
            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
            thread_local_allocation_time.local()+= duration.count();
#endif
            return pointer;
        }

        void free(uintptr_t block, order_t order)
        {
#if TRACK_GARBAGE_RECORD_TIME
            auto start = std::chrono::high_resolution_clock::now();
#endif
            if (order < LARGE_BLOCK_THRESHOLD)
            {
                push(free_blocks.local(), order, block);
            }
            else
            {
                std::lock_guard<std::mutex> lock(mutex);
                // printf("block ptr is %lu order is %c\n",block,order);
                push(large_free_blocks, order, block);
            }
#if TRACK_GARBAGE_RECORD_TIME
            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
            thread_local_free_time.local()+= duration.count();
#endif
        }

        template <typename T> inline T *convert(uintptr_t block)
        {
            if (__builtin_expect((block == NULLPOINTER), 0))
                return nullptr;
            // T* ptr = reinterpret_cast<T *>(reinterpret_cast<char *>(data) + block);
            return reinterpret_cast<T *>(reinterpret_cast<char *>(data) + block);
        }

#if TRACK_GARBAGE_RECORD_TIME
        inline void record_garbage_record_time(uint64_t garbage_collect_time){
            thread_local_garbage_record_time.local()+= garbage_collect_time;
        }
        inline void print_avg_garbage_record_time(){
            uint64_t thread_num = thread_local_garbage_record_time.size();
            uint64_t total_time = 0;
            for(auto t : thread_local_garbage_record_time){
                total_time+= t;
            }
            std::cout<<"avg garbage record time is "<<total_time/thread_num<<" us"<<std::endl;
            thread_local_garbage_record_time.clear();
        }
        inline void print_avg_alloc_time(){
            uint64_t thread_num = thread_local_allocation_time.size();
            uint64_t total_time = 0;
            for(auto t: thread_local_allocation_time){
                total_time+=t;
            }
            std::cout<<"avg memory alloc time is "<<total_time/thread_num<<" us"<<std::endl;
            thread_local_allocation_time.clear();
        }
        inline void print_avg_free_time(){
            uint64_t thread_num = thread_local_free_time.size();
            uint64_t total_time = 0;
            for(auto t: thread_local_free_time){
                total_time += t;
            }
            std::cout<<"avg memory free time is "<<total_time/thread_num<<" us"<<std::endl;
            thread_local_free_time.clear();
        }
#endif
    private:
        const size_t capacity;
        int fd;
        void *data;
        std::mutex mutex;
        tbb::enumerable_thread_specific<std::vector<std::vector<uintptr_t>>> free_blocks;
        std::vector<std::vector<uintptr_t>> large_free_blocks;
        std::atomic<size_t> used_size, file_size;
        uintptr_t null_holder;
#if TRACK_GARBAGE_RECORD_TIME
        tbb::enumerable_thread_specific<uint64_t> thread_local_garbage_record_time;
        tbb::enumerable_thread_specific<uint64_t> thread_local_allocation_time;
        tbb::enumerable_thread_specific<uint64_t> thread_local_free_time;
#endif
        uintptr_t pop(std::vector<std::vector<uintptr_t>> &free_block, order_t order)
        {
            uintptr_t pointer = NULLPOINTER;
            if (free_block[order].size())
            {
                pointer = free_block[order].back();
                free_block[order].pop_back();
            }
            return pointer;
        }

        void push(std::vector<std::vector<uintptr_t>> &free_block, order_t order, uintptr_t pointer)
        {
            free_block[order].push_back(pointer);
        }

        constexpr static int EMPTY_FD = -1;
        //constexpr static order_t MAX_ORDER = 64;
        constexpr static order_t MAX_ORDER = 32;
        constexpr static order_t LARGE_BLOCK_THRESHOLD = 20;
        constexpr static size_t FILE_TRUNC_SIZE = 1ul << 30; // 1GB
    };

 /*   class BlockManagerLibc
    {
    public:
        constexpr static uintptr_t NULLPOINTER = UINTPTR_MAX;

        uintptr_t alloc(order_t order)
        {
            auto p = aligned_alloc(1ul << order, 1ul << order);
            if (!p){
                printf(" Value of errno: %d\n ", errno);
                throw std::runtime_error("Failed to alloc block");
            }
            return reinterpret_cast<std::uintptr_t>(p);
        }

        void free(uintptr_t block, order_t order) { ::free(reinterpret_cast<void *>(block)); }

        template <typename T> T *convert(uintptr_t block)
        {
            if (block == NULLPOINTER)
                return nullptr;
            return reinterpret_cast<T *>(block);
        }
    };*/
}//namespace bwgraph
//#endif //BWGRAPH_V2_BLOCK_MANAGER_HPP
