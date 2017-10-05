#pragma once

#include "Transaction.hh"
#include "rwlock.hh"

#define MAX_NTHREADS 128
#define MAX_RANKS 128
#define ABORTED_STATE 1
#define COMMITTED_STATE 2
#define INVALID -1

struct TxnInfo;
struct PieceInfo;
struct RankInfo;

struct PieceInfo {
public:
    TxnInfo* owner;
    int txn_num;
    int rank;
    bool aborted;

    unsigned nreads;
    void** read_keys;
    // technically we don't need this because we're
    // not aborting pieces, but instead aborting txns
    unsigned* writeset;
    unsigned nwrites;
    void** write_keys;
    
    PieceInfo(TxnInfo* newowner, unsigned newtnum, int newrank) : 
        owner(newowner), txn_num(newtnum), rank(newrank) {
        aborted = false;
        read_keys = write_keys = nullptr;
        writeset = nullptr;
        nwrites = nreads = 0;
    }
};

struct TxnInfo {
public:
    std::vector<PieceInfo*> pieces;
    PieceInfo* active_piece;
    int txn_num; // keeps track of 'which' txn we're executing
    bool should_abort;
    rwlock txn_lk;

    std::vector<std::pair<TxnInfo*, int>> forward_deps;
    std::vector<std::pair<TxnInfo*, int>> backward_deps;

    TxnInfo() : active_piece(nullptr), txn_num(0), should_abort(false) {}

    void abort_dependent_txns() {
        // abort anyone who's dependent and who hasn't aborted yet
        for (auto& pair : backward_deps) {
            if (pair.second != INVALID) {
                auto& txn = pair.first;
                bool same_txn = (txn->txn_num == pair.second);
                if (same_txn) {
                    txn->set_should_abort();
                } else {
                    pair.second = INVALID;
                }
            }
        }
    }
    // used to protect accesses to txn_num
    void lock() {
        txn_lk.write_lock();
    }
    void unlock() {
        txn_lk.write_unlock();
    }
    void set_should_abort() {
        should_abort = true;
    }
};

struct RankInfo {
public:
    unsigned rank;
    PieceInfo* rank_pieces[MAX_NTHREADS];
    rwlock rank_lk;

    RankInfo() {
        rank_lk = rwlock();
    };
    
    void lock() {
        rank_lk.write_lock();
    }
    void unlock() {
        rank_lk.write_unlock();
    }
};

TxnInfo tinfos_[MAX_NTHREADS];
RankInfo rankinfos_[MAX_RANKS];

class ChoppedTransaction : public Sto {
public:
    static void start_txn() {
        Sto::start_transaction();
    }

    static void end_txn() {
        auto& txn = tinfos_[TThread::id()];
        // wait until txns of forward dep have committed to commit
        for (unsigned i = 0; i < txn.forward_deps.size(); ++i) {
            auto pair = txn.forward_deps.back(); 
            auto ftxn = pair.first;
            auto tnum = pair.second;
            if (tnum == INVALID) {
                continue;
            }
            // we don't have to lock when we check these
            // because ftxn can only ever become valid (a "monotonic" relation)
            while (ftxn->txn_num == tnum && ftxn->active_piece) {
                ftxn->unlock();
                sched_yield();
                ftxn->lock();
            }
            if (ftxn->txn_num != tnum) {
                pair.second = INVALID;
            }
            ftxn->unlock();
        }
        // need to check if during our wait, we were told to abort
        if (txn.should_abort) {
            abort_txn(&txn);
        } else {
            txn.lock();
            txn.txn_num++; //people will see this and know we committed
            txn.unlock();
            txn.forward_deps.clear();
            txn.backward_deps.clear();
            txn.active_piece = nullptr; 
            txn.should_abort = false;
            for (auto piece : txn.pieces) {
                rankinfos_[piece->rank].lock();
                rankinfos_[piece->rank].rank_pieces[TThread::id()] = nullptr;
                rankinfos_[piece->rank].unlock();
                Transaction::rcu_free(piece->writeset);
                Transaction::rcu_free(piece->read_keys);
                Transaction::rcu_free(piece->write_keys);
                Transaction::rcu_delete(piece);
            }
            txn.pieces.clear();
        }
        Sto::set_state(!txn.should_abort);
    }
 
    static void start_piece(int rank) {
        auto& txn = tinfos_[TThread::id()];
       
        // enforce monotonic rank ordering
        if (!txn.pieces.empty()) {
            auto last_piece = txn.pieces.back();
            assert(rank > last_piece->rank);
        }

        // create new piece 
        PieceInfo* pi = new PieceInfo(&txn, txn.txn_num, rank);
        txn.active_piece = pi;
        txn.pieces.push_back(pi);

        // wait for any other transactions who are executing on this rank
        rankinfos_[rank].lock();

        // wait until txns of forward dep have past this rank or committed
        for (unsigned i = 0; i < txn.forward_deps.size(); ++i) {
            auto pair = txn.forward_deps.back(); 
            auto ftxn = pair.first;
            auto tnum = pair.second;
            if (tnum == INVALID) {
                continue;
            }
            // we don't have to lock when we check these
            // because ftxn can only ever become ok (a "monotonic" relation)
            while (ftxn->txn_num == tnum && ftxn->active_piece && 
                    (ftxn->active_piece->rank <= rank)) {
                ftxn->unlock();
                sched_yield();
                ftxn->lock();
            }
            if (ftxn->txn_num != tnum) {
                pair.second = INVALID;
            }
            ftxn->unlock();
        }
        
        // check if we have been told to abort before we actually start executing the piece
        if (txn.should_abort) {
            abort_txn(&txn);
        }
    }

    static void abort_txn(TxnInfo* txn) {
        assert(txn->should_abort);
        for (auto pi : txn->pieces) {
            pi->aborted = true; // XXX locking might be a bit off?
        }
        // TODO actually abort the transaction
        
        txn->lock();
        txn->txn_num++;
        txn->unlock();
        
        // no one will add back dependencies any more
        txn->abort_dependent_txns();
        
        txn->forward_deps.clear();
        txn->backward_deps.clear();
        txn->active_piece = nullptr; 
        txn->should_abort = false;
        for (auto piece : txn->pieces) {
            rankinfos_[piece->rank].lock();
            rankinfos_[piece->rank].rank_pieces[TThread::id()] = nullptr;
            rankinfos_[piece->rank].unlock();
            Transaction::rcu_free(piece->writeset);
            Transaction::rcu_free(piece->read_keys);
            Transaction::rcu_free(piece->write_keys);
            Transaction::rcu_delete(piece);
        }
        txn->pieces.clear();
    }

    static bool try_commit_piece() {
        auto& txn = tinfos_[TThread::id()];
        auto& piece = txn.active_piece;
        auto& rank = piece->rank;

        // ensure that no other transaction of the same rank can commit (and add deps)
        // so we know that we're checking all the possible pieces that we could have depended on
        // XXX we already locked this
        //rankinfos_[rank].lock();
        
        // this updates the piece info with the relevant info
        bool committed = Sto::try_commit_piece(
                    piece->writeset, 
                    piece->write_keys, 
                    piece->read_keys,
                    piece->nwrites, 
                    piece->nreads);
        if (!committed) {
            assert(0); // for now
            rankinfos_[rank].unlock();
            return false;
        }

        // check for new dependencies
        // iterate through each piece of the same rank in rankinfos_, check for overlaps in r/ws
        for (int i = 0; i < MAX_NTHREADS; ++i) {
            auto pi = rankinfos_[piece->rank].rank_pieces[i];
            // lock the owner txn. this means that the txn cannot abort / others cannot add 
            // backward dependencies while we might. 
            if (pi) {
                pi->owner->lock();
                if (overlap(pi, piece)) {
                    if (pi->owner->txn_num != pi->txn_num) {
                        if(pi->aborted) {
                            // the txn has already aborted but the piece has not yet been removed
                            // conservatively abort because we might have seen some of the piece changes
                            pi->owner->unlock();
                            abort_txn(&txn);
                        } else {
                            // the txn committed
                            pi->owner->unlock();
                            continue;
                        }
                    }
                    // the piece data is still for an active txn
                    // add dependency
                    pi->owner->backward_deps.push_back(std::make_pair(piece->owner, piece->txn_num));
                    txn.forward_deps.push_back(std::make_pair(pi->owner, pi->txn_num));
                    pi->owner->unlock();
                }
            }
        }
        // update the rankinfo for other txns to see our reads/writes
        rankinfos_[rank].rank_pieces[TThread::id()] = piece;
        rankinfos_[rank].unlock();

        return committed;
    }

    static void commit_piece() {
        if (!try_commit_piece()) {
            throw Transaction::Abort();
        }
    }
  
    static bool overlap(PieceInfo* p1, PieceInfo* p2) {
        // can make this more efficient with something like a bloom filter
        void* key;
        // read-write
        for (unsigned i = 0; i < p2->nreads; ++i) {
            key = p2->read_keys[i];
            for (unsigned j = 0; j < p1->nwrites; ++j) {
                if (key == p1->write_keys[j]) {
                    return true;
                }
            }
        }
        // write-write
        for (unsigned i = 0; i < p2->nwrites; ++i) {
            key = p2->write_keys[i];
            for (unsigned j = 0; j < p1->nwrites; ++j) {
                if (key == p1->write_keys[j]) {
                    return true;
                }
            }
        }
        // write-read
        for (unsigned i = 0; i < p2->nwrites; ++i) {
            key = p2->write_keys[i];
            for (unsigned j = 0; j < p1->nreads; ++j) {
                if (key == p1->read_keys[j]) {
                    return true;
                }
            }
        }
        return false;
    }
};
