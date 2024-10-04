#include <iostream>
#include "core/block_manager.hpp"
#include "core/transaction_tables.hpp"
#include "core/block.hpp"
#include "core/bw_index.hpp"
#include "core/commit_manager.hpp"
#include "core/block_access_ts_table.hpp"
using namespace GTX;
int main() {
    GTX::BlockManager manager("", 1e+11);
    auto ptr = manager.alloc(5);
    std::cout << "Hello, World! " <<ptr<< std::endl;
    std::cout<<sizeof(ArrayTransactionTables)<<std::endl;
    std::cout<<sizeof(EdgeDeltaBlockHeader)<<std::endl;
    EdgeLabelBlock label_block;
    std::cout<<sizeof(label_block)<<std::endl;
    std::cout<<sizeof(VertexIndexBucket)<<std::endl;
    std::cout<<sizeof(CommitManager)<<std::endl;
    std::cout<<sizeof(BlockAccessTimestampTable)<<std::endl;
    return 0;
}
