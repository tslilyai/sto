#pragma once
#include "Transaction.hh"
#include "rwlock.hh"

#define N_THREADS 128
#define ABORTED_STATE 1
#define COMMITTED_STATE 2
#define INVALID -1

class ChoppedTransaction : public Sto {
public:
    static void start_txn() {
        Sto::start_transaction;
    }

    static void end_txn() {
        tinfos_[TThread::id()].end();
    }
 
    static void abort_txn() {
        tinfos_[TThread::id()].abort();
   }

    static void start_piece(int rank) {
        PieceInfo* last_piece;
        auto tid = TThread::id();
        auto txn = tinfos_[tid];
       
        // enforce rank ordering
        if (!txn.pieces.empty()) {
            auto last_piece = txn.pieces.back();
            assert(rank > last_piece.rank);
        }

        // create new piece 
        PieceInfo& pi = new PieceInfo(&txn, rank);
        txn.active_piece = pi;
        txn.pieces.push_back(pi);

        // wait until txns of forward dep have moved to or past this rank or committed
        for (unsigned i = 0; i < txn.forward_deps.size(); ++i) {
            auto pair : txn.forward_deps.back(); 
            auto ftxn = pair.first;
            auto tnum = pair.second;
            if (tnum = INVALID) {
                continue;
            }
            // we don't have to lock when we check these
            // because ftxn can only ever become valid (a "monotonic" relation)
            while (ftxn->txn_num == tnum && ftxn->active_piece && 
                    && ftxn->active_piece->rank <= rank) {
                ftxn->unlock();
                sched_yield();
                ftxn->lock();
            }
            if (ftxn->txn_num != tnum) {
                pair.second = INVALID;
            }
            ftxn->unlock();
        }
    }

    static bool try_commit_current_piece() {
        auto txn = tinfos_[TThread::id()];
        auto piece = txn.active_piece;
        auto rank = piece.rank;

        // ensure that no other transaction of the same rank can commit (and add deps)
        // so we know that we're checking all the possible pieces that we could have depended on
        rankinfos_[rank].lock();
        
        // this updates the piece info with the relevant info
        bool committed = Sto::try_commit_piece(
                    piece->writeset, 
                    piece->write_keys, 
                    piece->read_keys,
                    piece->nwrites, 
                    piece->nreads);
        if (!committed) {
            assert(0); // for now
            txn.should_abort = true;
            abort_txn();
            return false;
        }

        // check for new dependencies
        // iterate through each piece of the same rank in rankinfos_, check for overlaps in r/ws
        for (int i = 0; i < N_THREADS; ++i) {
            auto pi = rankinfos_[piece->rank][i];
            if (overlap(pi->write_keys, piece->write_keys)
                    || overlap(pi->read_keys, piece->write_keys)
                    || overlap(pi->write_keys, pi->read_keys)) {
                if (pi->aborted) {
                    rankinfos_[rank].unlock()
                    txn.set_should_abort();
                    abort_txn();
                    return false;
                } 
                piend->owner->lock();
                piend->owner->backward_deps.push_back(piece->owner);
                txn->forward_deps.push_back(piend->owner);
                piend->owner->unlock();
            }
            rankinfo[rank][TThread::id()] = pi;

            rankinfos_[rank].unlock();
            return committed;
        }    

    static void commit_current_piece() {
        if (!try_commit_current_piece()) {
            throw Transaction::Abort();
        }
    }

private:
   void abort_piece(PieceInfo* pi) {
        pi->set_aborted();
        // anyone who started during or before the abort has
        // an old pointer to pi and will see the aborted flag set
        Transaction::rcu_free pi;
    }
    
    struct PieceInfo {
        int rank;
        TransInfo* owner;
        bool aborted;

        unsigned nreads;
        void** read_keys;
        // technically we don't need this because we're
        // not aborting pieces, but instead aborting txns
        unsigned* writeset;
        unsigned nwrites;
        void** write_keys;

        PieceInfo(TransInfo* newowner, int newrank) : owner(newowner), rank(newrank) {
            aborted = false;
            read_keys = writeset = write_keys = nwrites = nreads = 0;
            forward_deps.clear();
            take_rankinfo_snapshot();
        }

        void set_aborted() {
            aborted = true;
        }
    }

    struct TxnInfo {
        std::vector<PieceInfo*> pieces;
        PieceInfo* active_piece;
        unsigned txn_num; // keeps track of 'which' txn we're executing
        bool should_abort;
        rwlock txn_lk;

        std::vector<std::pair<ThreadInfo*, unsigned>> forward_deps;
        std::vector<std::pair<ThreadInfo*, unsigned>> backward_deps;
 
        TxnInfo() : active_piece(nullptr), txn_num(0), should_abort(false) {}

        void end() {
            lock();
            for (auto piece : pieces) {
                Transaction::rcu_free(piece->writeset);
                Transaction::rcu_free(piece->readkeys);
                Transaction::rcu_free(piece->writekeys);
                Transaction::rcu_delete(piece);
            }
            pieces.clear();
            forward_deps.clear();
            backward_deps.clear();
            active_piece = nullptr; 
            should_abort = false;
            txn_num++;
            unlock();
        }

        void lock() {
            txn_lk.write_lock();
        }
        void unlock() {
            txn_lk.write_unlock();
        }
        void set_should_abort() {
            should_abort = true;
        }

        // we don't need to lock around this because no one
        // will be trying to add dependencies to us (should_abort
        // is set)
        void abort() {
            assert(should_abort);
            for (auto pi : pieces) {
                abort_piece(pi);
            }
            abort_dependent_txns();
            reset();
        }

        // need to lock around this
        // no deadlock because those txns aren't lock when they're adding 
        // backward deps
        void abort_dependent_txns() {
            // abort anyone who's dependent and who hasn't aborted yet
            for (auto& pair : backward_deps) {
                if (pair.second != INVALID) {
                    auto txn = pair.first;
                    txn->lock();
                    bool same_txn = (txn->txn_num == pair.second);
                    if (same_txn) {
                        txn->set_should_abort();
                    } else {
                        pair.second = INVALID;
                    }
                    txn->unlock();
                }
            }
        }
    }

    struct RankInfo {
        unsigned rank;
        PieceInfo*[N_THREADS] rank_pieces;
        rwlock rank_lk;

        RankInfo(unsigned my_rank) : rank(rank);
        
        void lock() {
            rank_lk.write_lock();
        }
        void unlock() {
            rank_unlock.write_unlock();
        }
    }

    TxnInfo tinfos_[N_THREADS];
    std::vector<RankInfo> rankinfos_;
};
