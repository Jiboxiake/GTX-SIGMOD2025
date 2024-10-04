//
// Created by zhou822 on 5/22/23.
//

//#ifndef BWGRAPH_V2_UTILS_HPP
//#define BWGRAPH_V2_UTILS_HPP
#pragma once

#include <string>

#include "types.hpp"
#define txnIDMask 0x00FFFFFFFFFFFFFF
#define TS_ID_MASK 0x8000000000000000
#define Not_TS_ID_MASK 0x7FFFFFFFFFFFFFFF
#define VERTEX_ID_MASK 0x0000FFFFFFFFFFFF
namespace GTX{
    inline order_t size_to_order(size_t size)
    {
        order_t order = (order_t)((size & (size - 1)) != 0);
        while (size > 1)
        {
            order += 1;
            size >>= 1;
        }
        return order;
    }
    inline bool is_txn_id(uint64_t txnID){
        if((txnID&TS_ID_MASK)){
            return true;
        }
        return false;
    }
    inline uint8_t get_threadID(uint64_t txnID){
        //return (txnID >> 56)&(~TS_ID_MASK);
        return static_cast<uint8_t>(((txnID&Not_TS_ID_MASK)>>56));
    }
    //first bit tells it is a txn id
    //next 7 bits tell the thread id
    //final 56 bits tell the local txn id
    inline uint64_t generate_txnID(uint8_t threadID,uint64_t txnID){
        return (((uint64_t)threadID)<<56)+(txnID&txnIDMask)+TS_ID_MASK;
    }
    inline uint64_t get_local_txn_id(uint64_t txnID){
        return ((txnID<<8)>>8);
    }
    inline uint64_t generate_block_id(vertex_t vertex_id, label_t label){
        return (static_cast<uint64_t>(label)<<48)|static_cast<uint64_t>(vertex_id);
    }
    inline std::pair<vertex_t, label_t> decompose_block_id(uint64_t block_id){
        return std::pair<vertex_t,label_t>((block_id&VERTEX_ID_MASK),(static_cast<label_t>(block_id>>48)));
    }
    inline void record_lazy_update_record(lazy_update_map* txn_lazy_update_records, uint64_t original_txn_id){
      //  std::cout<<original_txn_id<<std::endl;
      /*  if(!(original_txn_id&0x8000000000000000)){
            std::cout<<original_txn_id<<std::endl;
        }*/
        auto emplace_result = txn_lazy_update_records->try_emplace(original_txn_id,1);
        if(!emplace_result.second){
            emplace_result.first->second++;
        }
    }
    inline delta_chain_id_t calculate_owner_delta_chain_id(vertex_t vid, int32_t delta_chain_num){
        return static_cast<delta_chain_id_t>(vid%delta_chain_num);
    }
    inline uint64_t combine_offset(uint32_t delta_offset, uint32_t data_offset){
        return static_cast<uint64_t>(delta_offset)+ (static_cast<uint64_t>(data_offset)<<32);
    }
}
//#endif //BWGRAPH_V2_UTILS_HPP
