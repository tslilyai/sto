#include <string>
#include <iostream>
#include <assert.h>
#include <vector>
#include <random>
#include <map>
#include <sys/time.h>
#include "ChoppedTransaction.hh"
#include "Transaction.hh"
#include "VectorTester.hh"

#define NTRANS 1
#define MAX_OPS 10
#define MAX_VALUE 10 // Max value of integers used in data structures
#define N_THREADS 1 
#define CHOPPED_OPS 5

unsigned initial_seeds[64];
VectorTester<int> chopped_tester;
VectorTester<int> whole_tester;

template <typename T>
void* run_whole(void* x) {
    int me = *((int*) x);
    TThread::set_id(me);
   
    Rand transgen(initial_seeds[2*me], initial_seeds[2*me + 1]);
    std::uniform_int_distribution<long> slotdist(0, MAX_VALUE);

    for (int i = 0; i < NTRANS; ++i) {
        txn_record *tr = new txn_record;
        while (1) {
        Sto::start_transaction();
        try {
            tr->ops.clear();
            for (int j = 0; j < MAX_OPS; j++) {
                int op = slotdist(transgen) % 2;
                tr->ops.push_back(whole_tester.doOp(op, me, j, j));
            }

            if (Sto::try_commit()) {
#if PRINT_DEBUG
                TransactionTid::lock(lock);
                std::cout << "[" << me << "] committed " << Sto::commit_tid() << std::endl;
                TransactionTid::unlock(lock);
#endif
                txn_list[me][Sto::commit_tid()] = tr;
                break;
            } else {
#if PRINT_DEBUG
                TransactionTid::lock(lock); std::cout << "[" << me 
                    << "] aborted "<< std::endl; TransactionTid::unlock(lock);
#endif
            }
        } catch (Transaction::Abort e) {
#if PRINT_DEBUG
            TransactionTid::lock(lock); std::cout << "[" << me 
                << "] aborted "<< std::endl; TransactionTid::unlock(lock);
#endif
        }
        }
    }
    return NULL;
}

template <typename T>
void* run_chopped(void* x) {
    int me = *((int*) x);
    TThread::set_id(me);
   
    Rand transgen(initial_seeds[2*me], initial_seeds[2*me + 1]);
    std::uniform_int_distribution<long> slotdist(0, MAX_VALUE);

    for (int i = 0; i < NTRANS; ++i) {
        int rank = 0;
        txn_record *tr = new txn_record;
        while (1) {
        ChoppedTransaction::start_txn();
        ChoppedTransaction::start_piece(rank++);
        try {
            tr->ops.clear();
            for (int j = 0; j < MAX_OPS; j++) {
                if (j % CHOPPED_OPS == 0) {
                    assert(ChoppedTransaction::try_commit_piece());
                    ChoppedTransaction::start_piece(rank++);
                }
                int op = slotdist(transgen) % 2;
                tr->ops.push_back(chopped_tester.doOp(op, me, j, j));
            }

            if (ChoppedTransaction::try_commit_piece()) {
                ChoppedTransaction::end_txn();
#if PRINT_DEBUG
                TransactionTid::lock(lock);
                std::cout << "[" << me << "] committed " << Sto::commit_tid() << std::endl;
                TransactionTid::unlock(lock);
#endif
                txn_list[me][Sto::commit_tid()] = tr;
                break;
            } else {
#if PRINT_DEBUG
                TransactionTid::lock(lock); std::cout << "[" << me 
                    << "] aborted "<< std::endl; TransactionTid::unlock(lock);
#endif
                assert(0);
            }
        } catch (Transaction::Abort e) {
#if PRINT_DEBUG
            TransactionTid::lock(lock); std::cout << "[" << me 
                << "] aborted "<< std::endl; TransactionTid::unlock(lock);
#endif
        }
        }
    }
    return NULL;
}

template <typename T>
void startAndWaitChopped() {
    pthread_t tids[N_THREADS];
    T tiddata[N_THREADS];
    for (int i = 0; i < N_THREADS; ++i) {
        tiddata[i] = i;
        pthread_create(&tids[i], NULL, run_chopped<T>, &tiddata[i]);
    }
    
    for (int i = 0; i < N_THREADS; ++i) {
        pthread_join(tids[i], NULL);
    }
}

template <typename T>
void startAndWaitWhole() {
    pthread_t tids[N_THREADS];
    T tiddata[N_THREADS];
    for (int i = 0; i < N_THREADS; ++i) {
        tiddata[i] = i;
        pthread_create(&tids[i], NULL, run_whole<T>, &tiddata[i]);
    }
   
    for (int i = 0; i < N_THREADS; ++i) {
        pthread_join(tids[i], NULL);
    }
}

void print_time(struct timeval tv1, struct timeval tv2) {
    printf("%f\n", (tv2.tv_sec-tv1.tv_sec) + (tv2.tv_usec-tv1.tv_usec)/1000000.0);
}

void test_chopped() {
    chopped_tester.init();

    txn_list.clear();
    for (int i = 0; i < N_THREADS; i++) {
        txn_list.emplace_back();
    }
 
    struct timeval tv1,tv2;
    gettimeofday(&tv1, NULL);
    
    startAndWaitChopped<int>();
    
    gettimeofday(&tv2, NULL);
    printf("Chopped time: ");
    print_time(tv1, tv2);
    
#if STO_PROFILE_COUNTERS
    Transaction::print_stats();
    Transaction::clear_stats();
#endif

    // Check correctness
    std::map<uint64_t, txn_record*> combined_txn_list;
    for (int i = 0; i < N_THREADS; i++) {
        combined_txn_list.insert(txn_list[i].begin(), txn_list[i].end());
    }
    
    std::cout << "Single thread replay" << std::endl;
    gettimeofday(&tv1, NULL);
    
    std::map<uint64_t, txn_record*>::iterator it = combined_txn_list.begin();
    for(; it != combined_txn_list.end(); it++) {
        for (unsigned i = 0; i < it->second->ops.size(); i++) {
            chopped_tester.redoOp(it->second->ops[i]);
        }
    }
    
    gettimeofday(&tv2, NULL);
    printf("Serial time: ");
    print_time(tv1, tv2);
   
    chopped_tester.check();
}

void test_whole() {
    whole_tester.init();
 
    txn_list.clear();
    for (int i = 0; i < N_THREADS; i++) {
        txn_list.emplace_back();
    }

    struct timeval tv1,tv2;
    gettimeofday(&tv1, NULL);
    
    startAndWaitWhole<int>();
    
    gettimeofday(&tv2, NULL);
    printf("Whole time: ");
    print_time(tv1, tv2);
    
#if STO_PROFILE_COUNTERS
    Transaction::print_stats();
    {
        txp_counters tc = Transaction::txp_counters_combined();
        printf("total_n: %llu, total_r: %llu, total_w: %llu, total_searched: %llu, total_aborts: %llu (%llu aborts at commit time)\n", tc.p(txp_total_n), tc.p(txp_total_r), tc.p(txp_total_w), tc.p(txp_total_searched), tc.p(txp_total_aborts), tc.p(txp_commit_time_aborts));
    }
#endif

    /* Check correctness */
    std::map<uint64_t, txn_record*> combined_txn_list;
    for (int i = 0; i < N_THREADS; i++) {
        combined_txn_list.insert(txn_list[i].begin(), txn_list[i].end());
    }
    
    std::cout << "Single thread replay" << std::endl;
    gettimeofday(&tv1, NULL);
    
    std::map<uint64_t, txn_record *>::iterator it = combined_txn_list.begin();
    for(; it != combined_txn_list.end(); it++) {
        for (unsigned i = 0; i < it->second->ops.size(); i++) {
            whole_tester.redoOp(it->second->ops[i]);
        }
    }
    
    gettimeofday(&tv2, NULL);
    printf("Serial time: ");
    print_time(tv1, tv2);
   
    whole_tester.check();
}

int main() {
    std::ios_base::sync_with_stdio(true);
    assert(CONSISTENCY_CHECK); // set CONSISTENCY_CHECK in Transaction.hh
    lock = 0;

    pthread_t advancer;
    pthread_create(&advancer, NULL, Transaction::epoch_advancer, NULL);
    pthread_detach(advancer);

    test_chopped();
    Transaction::clear_stats();
    test_whole();
}
