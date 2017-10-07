#include <string>
#include <iostream>
#include <assert.h>
#include <vector>
#include <random>
#include <map>
#include "Transaction.hh"
#include "Vector.hh"

#define PRINT_DEBUG 1
#define VEC_SIZE 1000

#define READ 0
#define UPDATE 1 
#define SIZE 2 
#define PUSH 3
#define POP 4

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

struct op_record {
    // Keeps track of a transactional operation, arguments used, and any read data.
    int op;
    std::vector<int> args;
    std::vector<int> rdata;
};

struct txn_record {
    // keeps track of operations in a single transaction
    std::vector<op_record*> ops;
};

std::vector<std::map<uint64_t, txn_record *> > txn_list;

template <typename T>
class Tester {
public: 
    virtual ~Tester() {}
    // initialize the data structures
    virtual void init() = 0;
    // Perform a particular operation on the data structure.    
    virtual op_record* doOp(int op, int me, int key, T& val) = 0;
    // Redo a operation. This is called during serial execution.
    virtual void redoOp(op_record *op) = 0;
    // Checks that final state of the two data structures are the same.
    virtual void check() = 0;
};


template <typename T>
class VectorTester : Tester<T> {
public:
    VectorTester() {
        vec = new Vector<T, true>(VEC_SIZE);
        vec_check.resize((VEC_SIZE));
   }
    ~VectorTester() {
        delete vec;
    }
    
    void init() {
        for (int i = 0; i < VEC_SIZE; i++) {
            TRANSACTION {
                vec->push_back(VEC_SIZE - i);
                vec_check[i] = VEC_SIZE - i;
            } RETRY(false);
        }
    }
    
    op_record* doOp(int op, int me, int key, T& val) {
#if !PRINT_DEBUG
        (void)me;
#endif
        if (op == UPDATE) {
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
#endif
            op_record* rec = new op_record;
            rec->op = op;
            rec->args.push_back(key);
            rec->args.push_back(val);
            rec->rdata.push_back(outOfBounds);
            return rec;
        } else if (op == READ) {
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] try to read " << key << std::endl;
            TransactionTid::unlock(lock);
#endif
            int val = -1;
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
#endif
            op_record* rec = new op_record;
            rec->op = op;
            rec->args.push_back(key);
            rec->rdata.push_back(val);
            rec->rdata.push_back(outOfBounds);
            return rec;
        } else if (op == PUSH) {
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
            op_record* rec = new op_record;
            rec->op = op;
            rec->args.push_back(val);
            return rec;
        } else if (op == POP) {
#if PRINT_DEBUG
            TransactionTid::lock(lock);
            std::cout << "[" << me << "] try to pop " << std::endl;
            TransactionTid::unlock(lock);
#endif
            int val = -1;
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
            op_record* rec = new op_record;
            rec->op = op;
            rec->rdata.push_back(val);
            rec->rdata.push_back(sz > 0);
            return rec;

        } else if (op == SIZE) {
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
#endif
            op_record* rec = new op_record;
            rec->op = op;
            rec->rdata.push_back(sz);
            return rec;
        }
        return nullptr;
    }
    
    void redoOp(op_record* op) {
        if (op->op == UPDATE) {
            int key = op->args[0];
            int val = op->args[1];
            int size = vec_check.size();
            if (op->rdata[0]) { assert(key >= size); return; }
            assert(key < size);
            vec_check[key] = val;
#if PRINT_DEBUG
            std::cout << "[" << "redo" << "] update "  << key << " " << val << std::endl;
#endif
         } else if (op->op == READ) {
            int key = op->args[0];
            int size = vec_check.size();
            if (op->rdata[1]) { assert(key >= size); return; }
            assert(key < size);
            int val = vec_check[key];
#if PRINT_DEBUG
            std::cout << "[" << "redo" << "] read (1)"  << key << " " << val << std::endl;
#endif
             assert(val == op->rdata[0]);
        } else if (op->op == PUSH){
            int val = op->args[0];
            vec_check.push_back(val);
#if PRINT_DEBUG
            std::cout << "[" << "redo" << "] push "  << val << std::endl;
#endif
         } else if (op->op == POP) {
            int size = vec_check.size();
            if (!op->rdata[1]) { assert(size == 0); return;}
            assert(size > 0);
#if PRINT_DEBUG
             std::cout << "[" << "redo" << "] pop "  << vec_check[size-1] << std::endl;
#endif
             assert(vec_check[size - 1] == op->rdata[0]);
            vec_check.pop_back();
        } else if (op->op == SIZE) {
#if PRINT_DEBUG
             std::cout << "[" << "redo" << "] size "  << vec_check.size() << std::endl;
#endif
             assert((int)vec_check.size() == op->rdata[0]);
        }
    }
    
    void check() {
        int size;
        TRANSACTION {
            size = vec->size();
            assert(size == (int)vec_check.size());
        } RETRY(false);
        for (int i = 0; i < size; i++) {
            TRANSACTION {
#if PRINT_DEBUG
                std::cout << "[" << vec->transGet(i) << "]" << " " << vec_check[i] << std::endl;
#endif
                assert(vec->transGet(i) == vec_check[i]);
            } RETRY(false);
        }
    }
    Vector<T, true>* vec;
    std::vector<T> vec_check;
};
