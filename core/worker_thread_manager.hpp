//
// Created by zhou822 on 6/18/23.
//
#pragma once
//#ifndef BWGRAPH_V2_WORKER_THREAD_MANAGER_HPP
//#define BWGRAPH_V2_WORKER_THREAD_MANAGER_HPP
//todo:: 2 versions, one use parallel hashmap, the other uses tbb
#include "../Libraries/parallel_hashmap/phmap.h"
#include <thread>
#include <atomic>
#include "tbb/concurrent_hash_map.h"
namespace GTX{
    using thread_id =  std::thread::id;
   /* using ThreadIDMap = phmap::parallel_flat_hash_map<
            thread_id,
            uint8_t ,
            phmap::priv::hash_default_hash<thread_id>,
            phmap::priv::hash_default_eq<thread_id>,
            std::allocator<std::pair<const thread_id, uint8_t>>,
            12,
            std::mutex>;*/
   using ThreadIDMap = tbb::concurrent_hash_map<thread_id,uint8_t>;
    class WorkerThreadManager{
    public:
        //~WorkerThreadManager(){std::cout<<static_cast<uint32_t>(openmp_thread_id_allocation.load())<<std::endl;}
        inline uint64_t get_real_worker_thread_size(){
            return thread_id_map.size();
        }
        inline uint8_t get_worker_thread_id(){
            uint8_t thread_id;
            ThreadIDMap::const_accessor s_accessor;
            /*if(!thread_id_map.if_contains(std::this_thread::get_id(),[&thread_id](typename ThreadIDMap::value_type& pair){ thread_id = pair.second;})) {
                thread_id = global_thread_id_allocation.fetch_add(1);
                thread_id_map.try_emplace(std::this_thread::get_id(),thread_id);
            }*/
            if(thread_id_map.find(s_accessor, std::this_thread::get_id())){
                thread_id = s_accessor->second;
            }else{
                s_accessor.release();
                thread_id = global_thread_id_allocation.fetch_add(1,std::memory_order_acq_rel);
                thread_id_map.emplace(std::this_thread::get_id(),thread_id);
            }
            return thread_id;
        }
        inline void reset_worker_thread_id(){
            global_thread_id_allocation.store(0,std::memory_order_release);
            thread_id_map.clear();
            reset_openmp_thread_id();
        }
        inline void reset_openmp_thread_id(){
            openmp_thread_id_allocation.store(0,std::memory_order_release);
            //openmp_thread_id_map.clear();
        }
        inline uint8_t get_openmp_worker_thread_id(){
          /*  uint8_t thread_id;
            if(!openmp_thread_id_map.if_contains(std::this_thread::get_id(),[&thread_id](typename ThreadIDMap::value_type& pair){ thread_id = pair.second;})) {
                thread_id = openmp_thread_id_allocation.fetch_add(1)+global_thread_id_allocation.load();
                openmp_thread_id_map.try_emplace(std::this_thread::get_id(),thread_id);
            }
            return thread_id;*/
         /* if(openmp_thread_id_allocation.load(std::memory_order_acquire)>64){
              throw std::runtime_error("openmp thread ids error");
          }*/
        /* if(global_thread_id_allocation.load(std::memory_order_acquire)>1){
             std::cout<<"bad things happened"<<std::endl;
             throw std::runtime_error("bad memory id allocation");
         }
         if(openmp_thread_id_allocation.load(std::memory_order_acquire)>=64){
             std::cout<<"second bad things happened"<<std::endl;
             throw std::runtime_error("bad memory id allocation");
         }*/
          return openmp_thread_id_allocation.fetch_add(1,std::memory_order_acq_rel)+global_thread_id_allocation.load(std::memory_order_acquire);
        }
        inline void print_debug_stats(){
            std::cout<<"worker thread id is "<<static_cast<uint32_t>(global_thread_id_allocation.load())<<std::endl;
            std::cout<<"openmp thread id is "<<static_cast<uint32_t>(openmp_thread_id_allocation.load())<<std::endl;
        }

    private:
        //for normal read write and read only transactions
        ThreadIDMap thread_id_map;
        std::atomic_uint8_t global_thread_id_allocation = 0;//this number should be fixed after the initial setup of worker threads
        //for openmp threads
        //ThreadIDMap openmp_thread_id_map;
        std::atomic_uint8_t openmp_thread_id_allocation = 0;
    };
}//bwgraph

//#endif //BWGRAPH_V2_WORKER_THREAD_MANAGER_HPP
