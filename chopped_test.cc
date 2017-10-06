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

#define NTRANS 1000
#define MAX_OPS 200
#define MAX_VALUE 10 // Max value of integers used in data structures
#define N_THREADS 15
#define CHOPPED_OPS 20

unsigned initial_seeds[64];
VectorTester<int> tester;

template <typename T>
void* run_whole(void* x) {
    int me = *((int*) x);
    TThread::set_id(me);
   
    Rand transgen(initial_seeds[2*me], initial_seeds[2*me + 1]);
    std::uniform_int_distribution<long> slotdist(0, MAX_VALUE);

    for (int i = 0; i < NTRANS; ++i) {
        while (1) {
        Sto::start_transaction();
        try {
            for (int j = 0; j < MAX_OPS; j++) {
                int op = slotdist(transgen) % 2;
                tester.doOp(op, me, j, j);
            }

            if (Sto::try_commit()) {
#if PRINT_DEBUG
                TransactionTid::lock(lock);
                std::cout << "[" << me << "] committed " << numOps << " ops" << std::endl;
                TransactionTid::unlock(lock);
#endif
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
        while (1) {
        ChoppedTransaction::start_txn();
        ChoppedTransaction::start_piece(rank++);
        try {
            for (int j = 0; j < MAX_OPS; j++) {
                if (j % CHOPPED_OPS == 0) {
                    assert(ChoppedTransaction::try_commit_piece());
                    ChoppedTransaction::start_piece(rank++);
                }
                int op = slotdist(transgen) % 2;
                tester.doOp(op, me, j, j);
            }

            if (ChoppedTransaction::try_commit_piece()) {
#if PRINT_DEBUG
                TransactionTid::lock(lock);
                std::cout << "[" << me << "] committed " << numOps << " ops" << std::endl;
                TransactionTid::unlock(lock);
#endif
                ChoppedTransaction::end_txn();
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

int main() {
    std::ios_base::sync_with_stdio(true);
    //assert(CONSISTENCY_CHECK); // set CONSISTENCY_CHECK in Transaction.hh
    lock = 0;

    tester.init();

    pthread_t advancer;
    pthread_create(&advancer, NULL, Transaction::epoch_advancer, NULL);
    pthread_detach(advancer);
 
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

    gettimeofday(&tv1, NULL);
    
    startAndWaitWhole<int>();
    
    gettimeofday(&tv2, NULL);
    printf("Parallel time: ");
    print_time(tv1, tv2);
    
#if STO_PROFILE_COUNTERS
    Transaction::print_stats();
    {
        txp_counters tc = Transaction::txp_counters_combined();
        printf("total_n: %llu, total_r: %llu, total_w: %llu, total_searched: %llu, total_aborts: %llu (%llu aborts at commit time)\n", tc.p(txp_total_n), tc.p(txp_total_r), tc.p(txp_total_w), tc.p(txp_total_searched), tc.p(txp_total_aborts), tc.p(txp_commit_time_aborts));
    }
#endif
}
