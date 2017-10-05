#pragma once
#include "Transaction.hh"
#include "rwlock.hh"

#define N_THREADS 128
#define ABORTED_STATE 1
#define COMMITTED_STATE 2
#define INVALID -1

class StoWrapper : public Sto {
public:
    static void start_txn() {
        Sto::start_transaction;
        auto tid = TThread::id();
    }

    static void end_txn() {
        auto txn = tinfos_[TThread::id()]
        txn->lock();
        reset();
        txn->unlock();
    }
 
    static void abort_txn() {
        auto txn = tinfos_[TThread::id()];
        txn.abort();
   }

    static void start_piece(int rank) {
        PieceInfo* last_piece;
        auto tid = TThread::id();
        auto txn = tinfos_[tid];
       
        // ensure that the txn can't abort while we start
        txn.lock();

        // enforce rank ordering
        if (!txn.pieces.empty()) {
            auto last_piece = txn.pieces.back();
            assert(rank > last_piece.rank);
        }

        // create new piece 
        PieceInfo& pi = new PieceInfo(&txn, rank);
        rankinfo[rank][TThread::id()] = pi;
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
            // because there are no dependency cycles, we can lock here!
            ftxn->lock();
            // XXX not sure if this is the right way to block...
            while (ftxn->txn_num == tnum && ftxn->active_piece && 
                    && ftxn->active_piece->rank < rank) {
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
        // lock piece to prevent someone from missing a dependency
        piece->wlock();
        
        // TODO implement this...
        // this updates the piece info with the relevant info
        bool committed = Sto::try_commit_piece(
                    piece->writeset, 
                    piece->readset, 
                    piece->write_keys, 
                    piece->read_keys,
                    piece->nwrites, 
                    piece->nreads);
        // safe to unlock here because r/w sets have been set 
        piece->wunlock();
        if (!committed) {
            txn.should_abort = true;
            abort_txn();
            return false;
        }

        // check for new dependencies
        // iterate through each piece of the same rank in rankinfos_, check for overlaps in r/ws
        for (int i = 0; i < N_THREADS; ++i) {
            auto pibegin = rankinfo_snapshot[i];
            auto pinow = rankinfos_[piece->rank][i];

            // this piece was replaced, so either the txn committed or aborted
            // check if it had aborted, and if so, if we need to abort
            // this is slightly conservative because the undo might have happened
            // before we ran 
            if (pibegin != pinow) {
                if (pibegin->aborted) {
                    if (overlap(pibegin->write_keys, piece->write_keys)
                            || overlap(pibegin->read_keys, piece->write_keys)
                            || overlap(pibegin->write_keys, pibegin->read_keys)) {
                        txn.set_should_abort();
                        abort_txn();
                        return false;
                    }
                }
            }

            // the pieces are the same. so we know that the piece has not
            // yet aborted. but it could abort after we lock it, so check anyway
            piend->rlock();
            if (overlap(piend->write_keys, piece->write_keys)
                    || overlap(piend->read_keys, piece->write_keys)
                    || overlap(piend->write_keys, piend->read_keys)) {
                if (piend->aborted) {
                    piend->runlock();
                    txn.set_should_abort();
                    abort_txn();
                    return false;
                } 
                piend->owner->lock();
                if (!piend->owner->should_abort) {
                    piend->owner->backward_deps.push_back(piece->owner);
                    txn->forward_deps.push_back(piend->owner);
                } else {
                    txn.set_should_abort();
                    abort_txn();
                    return false;
                }
                piend->owner->unlock();
                piend->runlock();
                return true;
            }
            active_piece->aborted = !committed; 
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
        // TODO implement this
        Sto::abort_piece(pi->writeset, pi->nwrites);
        // anyone who started during or before the abort has
        // an old pointer to pi and will see the aborted flag set
        Transaction::rcu_free pi;
    }
    
    struct PieceInfo {
        int rank;
        TransInfo* owner;
        bool aborted;

        unsigned* readset;
        unsigned nreads;
        void** read_keys;

        unsigned* writeset;
        unsigned nwrites;
        void** write_keys;

        rwlock piece_rwlk;
        PieceInfo*[N_THREADS] rankinfo_snapshot;

        PieceInfo(TransInfo* newowner, int newrank) : owner(newowner), rank(newrank) {
            aborted = false;
            readset = read_keys = writeset = write_keys = nwrites = nreads = 0;
            forward_deps.clear();
            take_rankinfo_snapshot();
        }

        void take_rankinfo_snapshot() {
            // XXX do we need to synchronize this? we're just using it to ensure
            // that a txn's piece doesn't abort and restart while we're executing
            memmove(rankinfos_[rank], rankinfo_snapshot, N_THREADS*sizeof(PieceInfo*));
        }

        void set_aborted() {
            wlock();
            aborted = true;
            wunlock();
        }
        
        void rlock() {
            piece_rwlk.read_lock();
        }
        void wlock() {
            piece_rwlk.write_lock();
        }
        void runlock() {
            piece_rwlk.read_unlock();
        }
        void wunlock() {
            piece_rwlk.write_unlock();
        }
    }

    struct TxnInfo {
        std::vector<PieceInfo*> pieces;
        PieceInfo* active_piece;
        unsigned txn_num;
        bool should_abort;
        rwlock txn_lk;

        std::vector<std::pair<ThreadInfo*, unsigned>> forward_deps;
        std::vector<std::pair<ThreadInfo*, unsigned>> backward_deps;
 
        TxnInfo() : active_piece(nullptr), txn_num(0), should_abort(false) {}

        // txn must be locked
        void reset() {
            for (auto piece : pieces) {
                Transaction::rcu_free(piece);
            }
            pieces.clear();
            forward_deps.clear();
            backward_deps.clear();
            active_piece = nullptr; 
            should_abort = false;
            txn_num++;
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

    TxnInfo tinfos_[N_THREADS];
    std::vector<PieceInfo*[N_THREADS]> rankinfos_;
};
