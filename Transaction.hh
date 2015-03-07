#pragma once

#include <vector>
#include <algorithm>
#include <functional>
#include <memory>
#include <type_traits>
#include <unistd.h>

#define PERF_LOGGING 1
#define DETAILED_LOGGING 0
#define ASSERT_TX_SIZE 0

#if ASSERT_TX_SIZE
#if DETAILED_LOGGING
#  define TX_SIZE_LIMIT 20000
#  include <iostream>
#  include <cassert>
#else
#  error "ASSERT_TX_SIZE requires DETAILED_LOGGING!"
#endif
#endif

#if DETAILED_LOGGING
#if PERF_LOGGING
#else
#  error "DETAILED_LOGGING requires PERF_LOGGING!"
#endif
#endif

#include "config.h"

#define LOCAL_VECTOR 1

#define NOSORT 0

#define MAX_THREADS 8

// transaction performance counters
enum txp {
    // all logging levels
    txp_total_aborts = 0, txp_total_starts = 1,
    txp_commit_time_aborts = 2, txp_max_set = 3,
    // DETAILED_LOGGING only
    txp_total_n = 4, txp_total_r = 5, txp_total_w = 6, txp_total_searched = 7,

#if !PERF_LOGGING
    txp_count = 0
#elif !DETAILED_LOGGING
    txp_count = 4
#else
    txp_count = 8
#endif
};

template <int N> struct has_txp_struct {
    static bool test(int p) {
        return unsigned(p) < unsigned(N);
    }
};
template <> struct has_txp_struct<0> {
    static bool test(int) {
        return false;
    }
};
inline bool has_txp(int p) {
    return has_txp_struct<txp_count>::test(p);
}


#if LOCAL_VECTOR
#include "local_vector.hh"
#endif

#include "Interface.hh"
#include "TransItem.hh"

#define INIT_SET_SIZE 512

void reportPerf();
#define STO_SHUTDOWN() reportPerf()

struct __attribute__((aligned(128))) threadinfo_t {
  unsigned epoch;
  unsigned spin_lock;
  std::vector<std::pair<unsigned, std::function<void(void)>>> callbacks;
  std::function<void(void)> trans_start_callback;
  std::function<void(void)> trans_end_callback;
  uint64_t p_[txp_count];
  threadinfo_t() : epoch(), spin_lock() {
      for (int i = 0; i != txp_count; ++i)
          p_[i] = 0;
  }
  static bool p_is_max(int p) {
      return p == txp_max_set;
  }
  unsigned long long p(int p) {
      return has_txp(p) ? p_[p] : 0;
  }
  void inc_p(int p) {
      add_p(p, 1);
  }
  void add_p(int p, uint64_t n) {
      if (has_txp(p))
          p_[p] += n;
  }
  void max_p(int p, unsigned long long n) {
      if (has_txp(p) && n > p_[p])
          p_[p] = n;
  }
  void combine_p(int p, unsigned long long n) {
      if (has_txp(p)) {
          if (!p_is_max(p))
              p_[p] += n;
          else if (n > p_[p])
              p_[p] = n;
      }
  }
};


// TransactionBuffer
template <typename T> struct ObjectDestroyer {
    static void destroy(void* object) {
        ((T*) object)->~T();
    }
};

class TransactionBuffer {
    struct elt;
    struct item;

  public:
    TransactionBuffer()
        : e_() {
    }
    ~TransactionBuffer() {
        if (e_)
            hard_clear(true);
    }

    static constexpr size_t aligned_size(size_t x) {
        return (x + 7) & ~7;
    }

    template <typename T>
    void* pack_unique(T x) {
        return pack_unique(std::move(x), (typename Packer<T>::is_simple_type) 0);
    }
    template <typename T>
    void* pack(T x) {
        return pack(std::move(x), (typename Packer<T>::is_simple_type) 0);
    }

    void clear() {
        if (e_)
            hard_clear(false);
    }

  private:
    struct itemhdr {
        void (*destroyer)(void*);
        size_t size;
    };
    struct item : public itemhdr {
        char buf[0];
    };
    struct elthdr {
        elt* next;
        size_t pos;
        size_t size;
    };
    struct elt : public elthdr {
        char buf[0];
        void clear() {
            size_t off = 0;
            while (off < pos) {
                itemhdr* i = (itemhdr*) &buf[off];
                i->destroyer(i + 1);
                off += i->size;
            }
            pos = 0;
        }
    };
    elt* e_;

    item* get_space(size_t needed) {
        if (!e_ || e_->pos + needed > e_->size) {
            size_t s = std::max(needed, e_ ? e_->size * 2 : 256);
            elt* ne = (elt*) new char[sizeof(elthdr) + s];
            ne->next = e_;
            ne->pos = 0;
            ne->size = s;
            e_ = ne;
        }
        e_->pos += needed;
        return (item*) &e_->buf[e_->pos - needed];
    }

    template <typename T>
    void* pack(T x, int) {
        return Packer<T>::pack(x);
    }
    template <typename T>
    void* pack(T x, void*) {
        size_t isize = aligned_size(sizeof(itemhdr) + sizeof(T));
        item* space = this->get_space(isize);
        space->destroyer = ObjectDestroyer<T>::destroy;
        space->size = isize;
        new (&space->buf[0]) T(std::move(x));
        return &space->buf[0];
    }

    template <typename T>
    void* pack_unique(T x, int) {
        return Packer<T>::pack(x);
    }
    template <typename T>
    void* pack_unique(T x, void*);

    void hard_clear(bool delete_all);
};

template <typename T>
void* TransactionBuffer::pack_unique(T x, void*) {
    void (*destroyer)(void*) = ObjectDestroyer<T>::destroy;
    for (elt* e = e_; e; e = e->next) {
        size_t off = 0;
        while (off < e->pos) {
            item* i = (item*) &e->buf[off];
            if (i->destroyer == destroyer
                && ((Aliasable<T>*) &i->buf[0])->x == x)
                return &i->buf[0];
            off += i->size;
        }
    }
    return pack(std::move(x));
}


class Transaction {
public:
  static threadinfo_t tinfo[MAX_THREADS];
  static __thread int threadid;
  static unsigned global_epoch;
  static __thread Transaction *__transaction;

  static Transaction& get_transaction() {
    if (!__transaction)
      __transaction = new Transaction();
    else
      __transaction->reset();
    return *__transaction;
  }

  static std::function<void(unsigned)> epoch_advance_callback;

  static threadinfo_t tinfo_combined() {
    threadinfo_t out;
    for (int i = 0; i != MAX_THREADS; ++i) {
        for (int p = 0; p != txp_count; ++p)
            out.combine_p(p, tinfo[i].p(p));
    }
    return out;
  }

  static void print_stats();


  static void acquire_spinlock(unsigned& spin_lock) {
    unsigned cur;
    while (1) {
      cur = spin_lock;
      if (cur == 0 && bool_cmpxchg(&spin_lock, cur, 1)) {
        break;
      }
      relax_fence();
    }
  }
  static void release_spinlock(unsigned& spin_lock) {
    spin_lock = 0;
    fence();
  }

  static void* epoch_advancer(void*) {
    while (1) {
      usleep(100000);
      auto g = global_epoch;
      for (auto&& t : tinfo) {
        if (t.epoch != 0 && t.epoch < g)
          g = t.epoch;
      }

      global_epoch = ++g;

      if (epoch_advance_callback)
        epoch_advance_callback(global_epoch);

      for (auto&& t : tinfo) {
        acquire_spinlock(t.spin_lock);
        auto deletetil = t.callbacks.begin();
        for (auto it = t.callbacks.begin(); it != t.callbacks.end(); ++it) {
          if (it->first <= g-2) {
            it->second();
            ++deletetil;
          } else {
            // callbacks are in ascending order so if this one is too soon of an epoch the rest will be too
            break;
          }
        }
        if (t.callbacks.begin() != deletetil) {
          t.callbacks.erase(t.callbacks.begin(), deletetil);
        }
        release_spinlock(t.spin_lock);
      }
    }
    return NULL;
  }

  static void cleanup(std::function<void(void)> callback) {
    acquire_spinlock(tinfo[threadid].spin_lock);
    tinfo[threadid].callbacks.emplace_back(global_epoch, callback);
    release_spinlock(tinfo[threadid].spin_lock);
  }

#if LOCAL_VECTOR
  typedef local_vector<TransItem, INIT_SET_SIZE> TransSet;
#else
  typedef std::vector<TransItem> TransSet;
#endif

  static void inc_p(int p) {
      add_p(p, 1);
  }
  static void add_p(int p, uint64_t n) {
      tinfo[threadid].add_p(p, n);
  }
  static void max_p(int p, unsigned long long n) {
      tinfo[threadid].max_p(p, n);
  }

  Transaction() : transSet_() {
#if !LOCAL_VECTOR
    transSet_.reserve(INIT_SET_SIZE);
#endif
    // TODO: assumes this thread is constantly running transactions
    tinfo[threadid].epoch = global_epoch;
    if (tinfo[threadid].trans_start_callback) tinfo[threadid].trans_start_callback();
    reset();
  }

  ~Transaction() {
    tinfo[threadid].epoch = 0;
    if (tinfo[threadid].trans_end_callback) tinfo[threadid].trans_end_callback();
    if (!isAborted_ && transSet_.size() != 0) {
      silent_abort();
    }
  }

  // reset data so we can be reused for another transaction
  void reset() {
    transSet_.clear();
    permute = NULL;
    perm_size = 0;
    may_duplicate_items_ = false;
    isAborted_ = false;
    firstWrite_ = -1;
    buf_.clear();
    if (tinfo[threadid].p(txp_total_aborts) % 0x10000 == 0xFFFF)
        print_stats();
    inc_p(txp_total_starts);
  }

  void consolidateReads() {
    // TODO: should be stable sort technically, but really we want to use insertion sort
    auto first = transSet_.begin();
    auto last = transSet_.end();
    std::sort(first, last);
    // takes the first element of any duplicates which is what we want. that is, we want to verify
    // only the oldest read
    transSet_.resize(std::unique(first, last) - first);
  }

  // adds item for a key that is known to be new (must NOT exist in the set)
  template <typename T>
  TransProxy new_item(Shared* s, T key) {
      transSet_.emplace_back(s, buf_.pack(std::move(key)));
      return TransProxy(*this, transSet_.back());
  }

  // adds item without checking its presence in the array
  template <typename T>
  TransProxy fresh_item(Shared *s, T key) {
      may_duplicate_items_ = true;
      transSet_.emplace_back(s, buf_.pack_unique(std::move(key)));
      return TransProxy(*this, transSet_.back());
  }

  // tries to find an existing item with this key, otherwise adds it
  template <typename T>
  TransProxy item(Shared* s, T key) {
      void* xkey = buf_.pack_unique(std::move(key));
      TransItem* ti = find_item<false>(s, xkey);
      if (!ti) {
          transSet_.emplace_back(s, xkey);
          ti = &transSet_.back();
      }
      return TransProxy(*this, *ti);
  }

  // gets an item that is intended to be read only. this method essentially allows for duplicate items
  // in the set in some cases
  template <typename T>
  TransProxy read_item(Shared *s, T key) {
      void* xkey = buf_.pack_unique(std::move(key));
      TransItem* ti = find_item<true>(s, xkey);
      if (!ti) {
          transSet_.emplace_back(s, xkey);
          ti = &transSet_.back();
      }
      return TransProxy(*this, *ti);
  }

  template <typename T>
  OptionalTransProxy check_item(Shared* s, T key) {
      void* xkey = buf_.pack_unique(std::move(key));
      return OptionalTransProxy(*this, find_item<false>(s, xkey));
  }

private:
  // tries to find an existing item with this key, returns NULL if not found
  template <bool read_only>
  TransItem* find_item(Shared *s, void* key) {
      if (read_only && firstWrite_ == -1) {
          may_duplicate_items_ = true;
          return NULL;
      }

      if (!read_only && firstWrite_ == -1) {
          consolidateReads();
      }

      for (auto it = transSet_.begin(); it != transSet_.end(); ++it) {
          TransItem& ti = *it;
          inc_p(txp_total_searched);
          if (ti.sharedObj() == s && ti.key_ == key)
              return &ti;
      }

      return NULL;
  }

public:
  typedef int item_index_type;
  item_index_type item_index(TransItem& ti) {
    return &ti - &transSet_[0];
  }

  void mark_write(TransItem& ti) {
    item_index_type idx = item_index(ti);
    if (firstWrite_ < 0 || idx < firstWrite_)
      firstWrite_ = idx;
  }

  bool check_for_write(TransItem& item) {
    // if permute is NULL, we're not in commit (just an opacity check), so no need to check our writes (we
    // haven't locked anything yet)
    if (!permute)
      return false;
    auto it = &item;
    bool has_write = it->has_write();
    if (!has_write && may_duplicate_items_) {
      has_write = std::binary_search(permute, permute + perm_size, -1, [&] (const int& i, const int& j) {
	  auto& e1 = unlikely(i < 0) ? item : transSet_[i];
	  auto& e2 = likely(j < 0) ? item : transSet_[j];
	  auto ret = likely(e1.key_ < e2.key_) || (unlikely(e1.key_ == e2.key_) && unlikely(e1.sharedObj() < e2.sharedObj()));
#if 0
	  if (likely(i >= 0)) {
	    auto cur = &i;
	    int idx;
	    if (ret) {
	      idx = (cur - permute) / 2;
	    } else {
	      idx = (permute + perm_size - cur) / 2;
	    }
	    __builtin_prefetch(&transSet_[idx]);
	  }
#endif
	  return ret;
	});
    }
    return has_write;
  }

  void check_reads() {
    if (!check_reads(&transSet_[0], &transSet_[transSet_.size()])) {
      abort();
    }
  }

  bool check_reads(TransItem *trans_first, TransItem *trans_last) {
    for (auto it = trans_first; it != trans_last; ++it)
      if (it->has_read()) {
        inc_p(txp_total_r);
        if (!it->sharedObj()->check(*it, *this)) {
          return false;
        }
      }
    return true;
  }

  bool commit() {
#if ASSERT_TX_SIZE
    if (transSet_.size() > TX_SIZE_LIMIT) {
        std::cerr << "transSet_ size at " << transSet_.size()
            << ", abort." << std::endl;
        assert(false);
    }
#endif
    max_p(txp_max_set, transSet_.size());
    add_p(txp_total_n, transSet_.size());

    if (isAborted_)
      return false;

    bool success = true;

    if (firstWrite_ == -1)
        firstWrite_ = transSet_.size();

    int permute_alloc[transSet_.size() - firstWrite_];
    permute = permute_alloc;

    //    int permute[transSet_.size() - firstWrite_];
    /*int*/ perm_size = 0;
    auto begin = &transSet_[0];
    auto end = begin + transSet_.size();
    for (auto it = begin + firstWrite_; it != end; ++it) {
      if (it->has_write()) {
	permute[perm_size++] = it - begin;
      }
    }

    //phase1
#if !NOSORT
    std::sort(permute, permute + perm_size, [&] (int i, int j) {
	return transSet_[i] < transSet_[j];
      });
#endif
    TransItem* trans_first = &transSet_[0];
    TransItem* trans_last = trans_first + transSet_.size();

    auto perm_end = permute + perm_size;
    for (auto it = permute; it != perm_end; ) {
      TransItem *me = &transSet_[*it];
      if (me->has_write()) {
        me->sharedObj()->lock(*me);
        ++it;
        if (may_duplicate_items_)
          for (; it != perm_end && transSet_[*it].same_item(*me); ++it)
            /* do nothing */;
      } else
        ++it;
    }

    /* fence(); */

    //phase2
    if (!check_reads(trans_first, trans_last)) {
      success = false;
      goto end;
    }

    //phase3
    for (auto it = trans_first + firstWrite_; it != trans_last; ++it) {
      TransItem& ti = *it;
      if (ti.has_write()) {
        inc_p(txp_total_w);
        ti.sharedObj()->install(ti);
      }
    }

  end:

    for (auto it = permute; it != perm_end; ) {
      TransItem *me = &transSet_[*it];
      if (me->has_write()) {
        me->sharedObj()->unlock(*me);
        ++it;
        if (may_duplicate_items_)
          for (; it != perm_end && transSet_[*it].same_item(*me); ++it)
            /* do nothing */;
      } else
        ++it;
    }

    if (success) {
      commitSuccess();
    } else {
      inc_p(txp_commit_time_aborts);
      abort();
    }

    transSet_.resize(0);

    return success;
  }

  void silent_abort() {
    if (isAborted_)
      return;
    inc_p(txp_total_aborts);
    isAborted_ = true;
    for (auto& ti : transSet_) {
      ti.sharedObj()->cleanup(ti, false);
    }
  }

  void abort() {
    silent_abort();
    throw Abort();
  }

  bool aborted() {
    return isAborted_;
  }

  class Abort {};

private:

  void commitSuccess() {
    for (TransItem& ti : transSet_) {
      ti.sharedObj()->cleanup(ti, true);
    }
  }

private:
    int firstWrite_;
    bool may_duplicate_items_;
    bool isAborted_;
    TransactionBuffer buf_;
    TransSet transSet_;
    int *permute;
    int perm_size;

    friend class TransProxy;
};



template <typename T>
TransProxy& TransProxy::add_read(T rdata) {
    if (!i_.shared.has_flags(READER_BIT)) {
        i_.shared.or_flags(READER_BIT);
        i_.rdata_ = t_.buf_.pack(std::move(rdata));
    }
    return *this;
}

template <typename T, typename U>
TransProxy& TransProxy::update_read(T old_rdata, U new_rdata) {
    if (i_.shared.has_flags(READER_BIT)
        && this->read_value<T>() == old_rdata)
        i_.rdata_ = t_.buf_.pack(std::move(new_rdata));
    return *this;
}

template <typename T>
TransProxy& TransProxy::add_write(T wdata) {
    if (has_write())
        // TODO: this assumes that a given writer data always has the same type.
        // this is certainly true now but we probably shouldn't assume this in general
        // (hopefully we'll have a system that can automatically call destructors and such
        // which will make our lives much easier)
        this->template write_value<T>() = std::move(wdata);
    else {
        i_.shared.or_flags(WRITER_BIT);
        i_.wdata_ = t_.buf_.pack(std::move(wdata));
        t_.mark_write(i_);
    }
    return *this;
}
