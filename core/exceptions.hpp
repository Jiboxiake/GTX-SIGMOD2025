//
// Created by zhou822 on 5/22/23.
//

//#ifndef BWGRAPH_V2_EXCEPTIONS_HPP
//#define BWGRAPH_V2_EXCEPTIONS_HPP
#pragma once
#include <iostream>
#include <exception>

namespace GTX {
    class DeltaLockException : public std::exception {
        virtual const char *what() const throw() {
            return "Under mutual exclusion lock only current transaction should be able to modify the offset";
        }
    };

    class DeltaChainCorruptionException : public std::exception {
        virtual const char *what() const throw() {
            return "Delta Chain should not contain ";
        }
    };

    class TransactionTableOpCountException : public std::exception {
        virtual const char *what() const throw() {
            return "the operation count should never be negative";
        }
    };

    class TransactionTableMissingEntryException : public std::exception {
        virtual const char *what() const throw() {
            return "transaction entry should stay until being fully lazy updated";
        }
    };

    class DeltaChainMismatchException : public std::exception {
        virtual const char *what() const throw() {
            return "wrong delta is stored in the delta chain";
        }
    };

    class LazyUpdateException : public std::exception {
        virtual const char *what() const throw() {
            return "lazy update exception";
        }
    };

    class LabelBlockPointerException : public std::exception {
        virtual const char *what() const throw() {
            return "Label Block should not have a next pointer unless all its entries are set";
        }
    };

    class BlockSafeAccessException : public std::exception {
        virtual const char *what() const throw() {
            return "the current thread should only check is_safe() iff it is accessing the block already";
        }
    };

    class LabelEntryMissingException : public std::exception {
        virtual const char *what() const throw() {
            return "If a transaction wrote to a label's block that label's entry should exist";
        }
    };

    class EdgeIteratorNoBlockToReadException : public std::exception {
        virtual const char *what() const throw() {
            return "If an edge delta block exists, there is at least one block that the current transaction can read";
        }
    };

    class LazyUpdateAbortException : public std::exception {
        virtual const char *what() const throw() {
            return "Lazy update will only update abort deltas during consolidation's installation phase. All other scenarios shall return ABORT";
        }
    };

    class GraphNullPointerException : public std::exception {
        virtual const char *what() const throw() {
            return "graph is having null pointers at a locations where it should not happen";
        }
    };

    class DeltaChainReclaimException : public std::exception {
        virtual const char *what() const throw() {
            return "consolidation did not capture all my in progress deltas";
        }
    };

    class EagerAbortException : public std::exception {
        virtual const char *what() const throw() {
            return "eager abort should never fail";
        }
    };

    class DeltaChainNumberException : public std::exception {
        virtual const char *what() const throw() {
            return "total delta chain number mismatch";
        }
    };

    class BlockStateException : public std::exception {
        virtual const char *what() const throw() {
            return "block state protocol exception";
        }
    };
    class ConsolidationException: public std::exception{
        virtual const char *what() const throw() {
            return "error took place during consolidation";
        }
    };
    class TransactionReadException: public std::exception{
        virtual const char *what() const throw() {
            return "reader transaction observes anomaly";
        }
    };
    class VertexDeltaException: public std::exception{
        virtual const char *what() const throw() {
            return "vertex delta anomaly";
        }
    };
    class LockReleaseException: public std::exception{
        virtual const char *what() const throw() {
            return "lock releasing is encountering error";
        }
    };
    class DeltaChainOffsetException: public std::exception{
        virtual const char *what() const throw() {
            return "inconsistent delta chain offset cache encountered";
        }
    };
    class ValidationException: public std::exception{
        virtual const char *what() const throw() {
            return "anomaly encountered during validation";
        }
    };
    class IllegalVertexAccessException: public std::exception{
        virtual const char *what() const throw() {
            return "accessing a vertex entry that is not allocated";
        }
    };
    class EagerCleanException: public std::exception{
        virtual const char *what() const throw() {
            return "eager clean never gets done";
        }
    };
    class CommitException: public std::exception{
        virtual const char *what() const throw() {
            return "group commit observes anomolies";
        }
    };
    class VertexCreationException: public std::exception{
        virtual const char *what() const throw() {
            return "trying to create a delta at an already-allocated entry";
        }
    };
}
//#endif //BWGRAPH_V2_EXCEPTIONS_HPP
