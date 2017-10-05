#include <string>
#include <iostream>
#include <assert.h>
#include <vector>
#include <random>
#include <map>
#include "Transaction.hh"
#include "List.hh"

#define MAX_VALUE 10 // Max value of integers used in data structures
#define N_THREADS 2

#define FIND 0
#define INSERT 1
#define DELETE 2
#define SIZE 3
#define ITER_BEGIN 3
#define ITER_HASNEXT 4
#define ITER_RESET 5
#define ITER_NEXT 6
#define ITER_NTHNEXT 7

struct Rand {
    typedef uint32_t result_type;
    
    result_type u, v;
    Rand(result_type u, result_type v) : u(u|1), v(v|1) {}
    
    inline result_type operator()() {
        v = 36969*(v & 65535) + (v >> 16);
        u = 18000*(u & 65535) + (u >> 16);
        return (v << 16) + u;
    }
    
    static constexpr result_type max() {
        return (uint32_t)-1;
    }
    
    static constexpr result_type min() {
        return 0;
    }
};

typedef TransactionTid::type Version;
Version lock;

template <typename T>
class Tester {
public: 
    virtual ~Tester() {}
    // initialize the data structures
    // structure under test
    virtual void init() = 0;
    // Perform a particular operation on the data structure.    
    virtual void doOp(int op, int me, T& val) = 0 ;
#if PRINT_DEBUG
    // Print stats for each data structure
    void print_stats(T* q) {};
#endif
};


template <typename T>
class ListTester : Tester<T> {
public:
    void init() {
        ls = new List<T>();
        for (int i = 0; i < 1000; i++) {
            TRANSACTION {
                ls->insert(i);
            } RETRY(false);
        }
    }
    
    void doOp(int op, int me, T& val) {
#if !PRINT_DEBUG
        (void)me;
#endif
        if (op == FIND) { // transFind
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] try find " << val << std::endl;
            TransactionTid::unlock(lock);
#endif
            bool success = ls->transFind(val);
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] found " << val << ": " << success << std::endl;
            TransactionTid::unlock(lock);
#endif
        } else if (op == INSERT) { //transInsert
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] try insert" << val << std::endl;
            TransactionTid::unlock(lock);
#endif
            bool success = ls->transInsert(val);
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] insert" << val << ": " << success << std::endl;
            TransactionTid::unlock(lock);
#endif
        } else if (op == DELETE) {
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] try to delete " << val << std::endl;
            TransactionTid::unlock(lock);
#endif
            bool success = ls->transDelete(val);
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] delete" << val << ": " << success << std::endl;
            TransactionTid::unlock(lock);
#endif
        } else if (op == SIZE) {
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] try to size" << std::endl;
            TransactionTid::unlock(lock);
#endif
            size_t size = ls->size();
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] size: " << size << std::endl;
            TransactionTid::unlock(lock);
#endif
        } else if (op == ITER_BEGIN) {
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] try iter_begin" << std::endl;
            TransactionTid::unlock(lock);
#endif
            *ls_iter = ls->transIter();
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] iter_begin: " << ls_iter << std::endl;
            TransactionTid::unlock(lock);
#endif
         } else if (op == ITER_RESET) {
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] try iter_reset" << std::endl;
            TransactionTid::unlock(lock);
#endif
            assert(ls_iter); 
            ls_iter->transReset();
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] iter_reset " << std::endl;
            TransactionTid::unlock(lock);
#endif
        } else if (op == ITER_HASNEXT) {
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] try iter_hasnext" << std::endl;
            TransactionTid::unlock(lock);
#endif
            assert(ls_iter); 
            bool yes = ls_iter->transHasNext();
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] iter_hasnext: " << yes << std::endl;
            TransactionTid::unlock(lock);
#endif
        } else if (op == ITER_NEXT) {
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] try iter_next" << std::endl;
            TransactionTid::unlock(lock);
#endif
            assert(ls_iter); 
            int* next = ls_iter->transNext();
            int nextval = next ? *next : -1;
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] iter_next: " << nextval << std::endl;
            TransactionTid::unlock(lock);
#endif
        } else if (op == ITER_NTHNEXT) {
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] try iter_nthnext" << std::endl;
            TransactionTid::unlock(lock);
#endif
            assert(ls_iter); 
            int* nthnext = ls_iter->transNthNext(val);
            int nthval = nthnext ? *nthnext : -1;
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] iter_nthnext: " << nthval << std::endl;
            TransactionTid::unlock(lock);
#endif
        } 
        return;
    }
    
#if PRINT_DEBUG
    void print_stats(T* ls) {
       return; 
    }
#endif
    
    static const int num_ops_ = 8;

private:
    List<T>* ls;
    typename List<T>::ListIter* ls_iter;
};
