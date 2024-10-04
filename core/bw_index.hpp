//
// Created by zhou822 on 5/27/23.
//
#pragma once
//#ifndef BWGRAPH_V2_BW_INDEX_HPP
//#define BWGRAPH_V2_BW_INDEX_HPP

#include "types.hpp"
#include "block.hpp"
#include "block_manager.hpp"
#include "utils.hpp"
namespace GTX {
#define BW_LABEL_BLOCK_SIZE 3
    class BlockManager;
    struct BwLabelEntry {
        BwLabelEntry(){}
        //EdgeDeltaBlockState state=EdgeDeltaBlockState::NORMAL;
        std::atomic<EdgeDeltaBlockState>state = EdgeDeltaBlockState::NORMAL;
        std::atomic_bool valid=false;
        label_t label;//label starts from 1
        uintptr_t block_ptr=0;//todo::check if we can directly use raw pointer
        std::atomic_uint64_t block_version_number = 0;//the last time consolidation took place
        std::vector<AtomicDeltaOffset>* delta_chain_index=nullptr;//this needs to be a pointer!
    };

    class EdgeLabelBlock {
    public:
        inline uint8_t get_offset(){
            return offset.load(std::memory_order_acquire);
        }
        inline void fill_information(vertex_t input_vid,BlockManager* input_block_mgr_ptr){
            owner_id= input_vid;
            block_manager=input_block_mgr_ptr;
        }
        //reader will locate a corresponding label entry if it exists, otherwise it returns false
        bool reader_lookup_label(label_t target_label,  BwLabelEntry*& target_entry);
        BwLabelEntry* writer_lookup_label(label_t target_label, TxnTables* txn_tables, timestamp_t txn_read_ts);
        BwLabelEntry* bulk_loader_lookup_label(label_t target_label,TxnTables* txn_tables, order_t order);

        void deallocate_all_delta_chains_indices();
        vertex_t owner_id;
        std::atomic_uint8_t offset=0;
        BlockManager* block_manager;
        //BwLabelEntry label_entries[BW_LABEL_BLOCK_SIZE];
        std::array<BwLabelEntry,BW_LABEL_BLOCK_SIZE> label_entries;
        std::atomic_uintptr_t next_ptr=0;
    };
    struct VertexIndexEntry{
        std::atomic_bool valid = false;
        std::atomic_uintptr_t vertex_delta_chain_head_ptr=0;
        std::atomic_uintptr_t edge_label_block_ptr=0;
        inline bool install_vertex_delta(uintptr_t current_delta_ptr, uintptr_t new_delta_ptr){
            return vertex_delta_chain_head_ptr.compare_exchange_strong(current_delta_ptr,new_delta_ptr,std::memory_order_acq_rel);
        }
    };
    class VertexIndexBucket{
    public:
        inline VertexIndexEntry& get_vertex_index_entry(vertex_t vid){
            //std::cout<<vid<<std::endl;
            //std::cout<<vid%static_cast<vertex_t>(BUCKET_SIZE)<<std::endl;
            return index_entries[vid%BUCKET_SIZE];
        }
        inline void allocate_vertex_index_entry(vertex_t vid){

            index_entries[vid%BUCKET_SIZE].valid.store(true,std::memory_order_release);
        }
    private:
        std::array<VertexIndexEntry,BUCKET_SIZE> index_entries;
    };
    struct BucketPointer{
    public:
        inline bool is_valid(){return valid.load(std::memory_order_acquire);}
        inline void allocate_block(VertexIndexBucket* input_bucket_ptr){
            index_bucket_ptr = input_bucket_ptr;
        }
        inline VertexIndexBucket* get_index_bucket_ptr(){return index_bucket_ptr;}//index bucket ptr should never be null when invoked
        inline void make_valid(){valid.store(true,std::memory_order_release);}
    private:
        std::atomic_bool valid = false;
        VertexIndexBucket* index_bucket_ptr= nullptr;
    };
    class VertexIndex{
    public:
        const size_t bucket_size = BUCKET_SIZE;
        VertexIndex(BlockManager& input_block_manager):global_vertex_id(1),block_manager(input_block_manager){
            auto new_bucket_ptr = block_manager.alloc(size_to_order(sizeof(VertexIndexBucket)));
            bucket_index[0].allocate_block(block_manager.convert<VertexIndexBucket>(new_bucket_ptr));
            bucket_index[0].make_valid();
        }
        inline vertex_t get_next_vid(){
            auto new_id =  global_vertex_id.fetch_add(1, std::memory_order_acq_rel);
            auto bucket_id = new_id / BUCKET_SIZE;
            if(!(new_id%BUCKET_SIZE)){
                auto new_bucket_ptr = block_manager.alloc(size_to_order(sizeof(VertexIndexBucket)));
                bucket_index[bucket_id].allocate_block(block_manager.convert<VertexIndexBucket>(new_bucket_ptr));
                bucket_index[bucket_id].make_valid();
            }
            else if(!bucket_index[bucket_id].is_valid()){
                while(!bucket_index[bucket_id].is_valid());
            }
            return new_id;
        }
        inline VertexIndexEntry& get_vertex_index_entry(vertex_t vid){
            return bucket_index[vid/BUCKET_SIZE].get_index_bucket_ptr()->get_vertex_index_entry(vid);
        }
        inline void make_valid(vertex_t vid){
            bucket_index[vid/BUCKET_SIZE].get_index_bucket_ptr()->get_vertex_index_entry(vid).valid.store(true, std::memory_order_release);
        }
        inline vertex_t get_current_allocated_vid(){
            return global_vertex_id.load(std::memory_order_acquire)-1;
        }
    private:
        std::atomic_uint64_t global_vertex_id;
        std::array<BucketPointer,BUCKET_NUM>bucket_index;
        BlockManager& block_manager;
    };
}
//#endif //BWGRAPH_V2_BW_INDEX_HPP
