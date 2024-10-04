//
// Created by zhou822 on 6/28/23.
//
#include <doctest/doctest/doctest.h>
#include <core/bw_index.hpp>
#include "core/bwgraph.hpp"
#include "core/gtx_transaction.hpp"
using namespace GTX;
TEST_CASE("BwGraph txn read own test"){
    BwGraph graph;
    auto txn = graph.begin_read_write_transaction();
    auto new_vid = txn.create_vertex();
    CHECK_EQ(new_vid,1);
    std::string v_data = "LZMNZ";
    txn.update_vertex(new_vid,v_data);
    std::string e_data0 = "edge0";
    std::string e_data1 = "edge1";
    std::string e_data2 = "edge2";
    txn.put_edge(new_vid, new_vid+1, 1, e_data0);
    txn.put_edge(new_vid, new_vid+2, 1, e_data1);
    txn.put_edge(new_vid, new_vid+3, 1, e_data2);
    //now let's do read
    auto v_data_read = txn.get_vertex(new_vid);
    CHECK_EQ(v_data,v_data_read);
    auto read_result0 = txn.get_edge(new_vid,new_vid+1,1);
    auto e_data_read0 = read_result0.second;
    CHECK_EQ(e_data_read0,e_data0);
    auto read_result1 = txn.get_edge(new_vid,new_vid+2,1);
    auto e_data_read1 = read_result1.second;
    CHECK_EQ(e_data_read1,e_data1);
    auto read_result2 = txn.get_edge(new_vid,new_vid+3,1);
    auto e_data_read2 = read_result2.second;
    CHECK_EQ(e_data_read2,e_data2);
}