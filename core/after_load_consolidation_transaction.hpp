//
// Created by zhou822 on 7/31/23.
//

#pragma once
#include "bwgraph.hpp"
namespace GTX{
    //currently assume only used under eager commit mode
    class AfterLoadCleanupTransaction{
        AfterLoadCleanupTransaction(BwGraph& input_graph, BlockManager& input_block_manager):graph(input_graph),block_manager(input_block_manager){}
        void consolidate_edge_delta_block(vertex_t vid, label_t label);
    private:
        BwGraph& graph;
        BlockManager& block_manager;
    };
}//bwgraph
