//
// Created by zhou822 on 6/9/23.
//

#include <doctest/doctest/doctest.h>
#include <core/bw_index.hpp>
#include "core/bwgraph.hpp"
#include "core/transaction_tables.hpp"
using namespace GTX;

TEST_CASE("Array Txn Table Test 1"){
    BwGraph graph;
    auto& txn_tables = graph.get_txn_tables();
    auto& table1 = txn_tables.get_table(0);
    auto txn_id1 = table1.generate_txn_id();
    CHECK_EQ(txn_id1,0x8000000000000000);
    uint64_t status;
    CHECK_FALSE(table1.get_status(txn_id1,status));
    auto txn1_entry = table1.put_entry(txn_id1);
    CHECK(table1.get_status(txn_id1,status));
    CHECK_EQ(status,IN_PROGRESS);
    txn_tables.commit_txn(txn1_entry,3,10);
    CHECK(txn_tables.get_status(txn_id1,status));
    CHECK_EQ(status,10);
    txn_tables.reduce_op_count(txn_id1,3);
    CHECK_EQ(txn1_entry->op_count,0);
    auto txn_id2 = table1.generate_txn_id();
    CHECK_EQ(txn_id2,0x8000000000000001);
}