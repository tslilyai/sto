#include <string>
#include <iostream>
#include <assert.h>
#include <vector>
#include <random>
#include <map>
#include "Transaction.hh"
#include "Vector.hh"

#define VEC_SIZE 1000

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
    virtual void doOp(int op, int me, int key, T& val) = 0 ;
#if PRINT_DEBUG
    // Print stats for each data structure
    void print_stats(T* q) {};
#endif
};

template <typename T>
class VectorTester : Tester<T> {
public:
    VectorTester() {
        vec = new Vector<T, true>(VEC_SIZE);
    }
    ~VectorTester() {
        delete vec;
    }
    
    void init() {
        for (int i = 0; i < VEC_SIZE; i++) {
            TRANSACTION {
                vec->push_back(i);
            } RETRY(false);
        }
    }
    
    void doOp(int op, int me, int key, T& val) {
#if !PRINT_DEBUG
        (void)me;
#endif
        if (op == 0) {
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] try to update " << key << ", " << val << std::endl;
            TransactionTid::unlock(lock);
#endif
            bool outOfBounds = false;
            try {
            vec->transUpdate(key, val);
            } catch (const std::out_of_range& e) {
                outOfBounds = true;
            }
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] update " << !outOfBounds << std::endl;
            TransactionTid::unlock(lock);
#else
            (void)outOfBounds;
#endif
        } else if (op == 1) {
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] try to read " << key << std::endl;
            TransactionTid::unlock(lock);
#endif
            bool outOfBounds = false;
            try {
            val = vec->transGet(key);
            } catch (std::out_of_range& e) {
                outOfBounds = true;
            }
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] read (" << !outOfBounds << ") " << key << ", " << val << std::endl;
            TransactionTid::unlock(lock);
#else
            (void)outOfBounds;
#endif
        } else if (op == 2) {
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] try to push " << val << std::endl;
            TransactionTid::unlock(lock);
#endif
            vec->push_back(val);
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] pushed " << val  << std::endl;
            TransactionTid::unlock(lock);
#endif
        } else if (op == 3) {
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] try to pop " << std::endl;
            TransactionTid::unlock(lock);
#endif
            int sz = vec->size();
            if (sz > 0) {
              val = vec->transGet(sz - 1);
              vec->pop_back();
            }
            
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] popped "  << sz-1 << " " << val << std::endl;
            TransactionTid::unlock(lock);
#endif
        } else {
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] try size " << std::endl;
            TransactionTid::unlock(lock);
#endif
            int sz = vec->size();
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] size "  << sz << std::endl;
            TransactionTid::unlock(lock);
#else
            (void)sz;
#endif
        }
    }
    
#if PRINT_DEBUG
    void print_stats(T* q) {
    }
#endif
    
    static const int num_ops_ = 5;
    Vector<T, true>* vec;
};
