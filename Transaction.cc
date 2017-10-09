#include "Transaction.hh"
#include <typeinfo>

Transaction::testing_type Transaction::testing;
threadinfo_t Transaction::tinfo[MAX_THREADS];
__thread int TThread::the_id;
Transaction::epoch_state __attribute__((aligned(128))) Transaction::global_epochs = {
    1, 0, TransactionTid::increment_value, true
};
__thread Transaction *TThread::txn = nullptr;
std::function<void(threadinfo_t::epoch_type)> Transaction::epoch_advance_callback;
TransactionTid::type __attribute__((aligned(128))) Transaction::_TID = 2 * TransactionTid::increment_value;
   // reserve TransactionTid::increment_value for prepopulated

static void __attribute__((used)) check_static_assertions() {
    static_assert(sizeof(threadinfo_t) % 128 == 0, "threadinfo is 2-cache-line aligned");
}

void Transaction::initialize() {
    static_assert(tset_initial_capacity % tset_chunk == 0, "tset_initial_capacity not an even multiple of tset_chunk");
    hash_base_ = 32768;
    tset_size_ = 0;
    lrng_state_ = 12897;
    for (unsigned i = 0; i != tset_initial_capacity / tset_chunk; ++i)
        tset_[i] = &tset0_[i * tset_chunk];
    for (unsigned i = tset_initial_capacity / tset_chunk; i != arraysize(tset_); ++i)
        tset_[i] = nullptr;
}

Transaction::~Transaction() {
    if (in_progress())
        silent_abort();
    TransItem* live = tset0_;
    for (unsigned i = 0; i != arraysize(tset_); ++i, live += tset_chunk)
        if (live != tset_[i])
            delete[] tset_[i];
}

void Transaction::refresh_tset_chunk() {
    assert(tset_size_ % tset_chunk == 0);
    assert(tset_size_ < tset_max_capacity);
    if (!tset_[tset_size_ / tset_chunk])
        tset_[tset_size_ / tset_chunk] = new TransItem[tset_chunk];
    tset_next_ = tset_[tset_size_ / tset_chunk];
}

void* Transaction::epoch_advancer(void*) {
    static int num_epoch_advancers = 0;
    if (fetch_and_add(&num_epoch_advancers, 1) != 0)
        std::cerr << "WARNING: more than one epoch_advancer thread\n";

    // don't bother epoch'ing til things have picked up
    usleep(100000);
    while (global_epochs.run) {
        epoch_type g = global_epochs.global_epoch;
        epoch_type e = g;
        for (auto& t : tinfo) {
            if (t.epoch != 0 && signed_epoch_type(t.epoch - e) < 0)
                e = t.epoch;
        }
        global_epochs.global_epoch = std::max(g + 1, epoch_type(1));
        global_epochs.active_epoch = e;
        global_epochs.recent_tid = Transaction::_TID;

        if (epoch_advance_callback)
            epoch_advance_callback(global_epochs.global_epoch);

        usleep(100000);
    }
    fetch_and_add(&num_epoch_advancers, -1);
    return NULL;
}

bool Transaction::preceding_duplicate_read(TransItem* needle) const {
    const TransItem* it = nullptr;
    for (unsigned tidx = 0; ; ++tidx) {
        it = (tidx % tset_chunk ? it + 1 : tset_[tidx / tset_chunk]);
        if (it == needle)
            return false;
        if (it->owner() == needle->owner() && it->key_ == needle->key_
            && it->has_read())
            return true;
    }
}

void Transaction::hard_check_opacity(TransItem* item, TransactionTid::type t) {
    // ignore opacity checks during commit; we're in the middle of checking
    // things anyway
    if (state_ == s_committing || state_ == s_committing_locked)
        return;

    // ignore if version hasn't changed
    if (item && item->has_read() && item->read_value<TransactionTid::type>() == t)
        return;

    // die on recursive opacity check; this is only possible for predicates
    if (unlikely(state_ == s_opacity_check)) {
        mark_abort_because(item, "recursive opacity check", t);
    abort:
        TXP_INCREMENT(txp_hco_abort);
        abort();
    }
    assert(state_ == s_in_progress);

    TXP_INCREMENT(txp_hco);
    if (TransactionTid::is_locked_elsewhere(t, threadid_)) {
        TXP_INCREMENT(txp_hco_lock);
        mark_abort_because(item, "locked", t);
        goto abort;
    }
    if (t & TransactionTid::nonopaque_bit)
        TXP_INCREMENT(txp_hco_invalid);

    state_ = s_opacity_check;
    start_tid_ = _TID;
    release_fence();
    TransItem* it = nullptr;
    for (unsigned tidx = 0; tidx != tset_size_; ++tidx) {
        it = (tidx % tset_chunk ? it + 1 : tset_[tidx / tset_chunk]);
        if (it->has_read()) {
            TXP_INCREMENT(txp_total_check_read);
            if (!it->owner()->check(*it, *this)
                && (!may_duplicate_items_ || !preceding_duplicate_read(it))) {
                mark_abort_because(item, "opacity check");
                goto abort;
            }
        } else if (it->has_predicate()) {
            TXP_INCREMENT(txp_total_check_predicate);
            if (!it->owner()->check_predicate(*it, *this, false)) {
                mark_abort_because(item, "opacity check_predicate");
                goto abort;
            }
        }
    }
    state_ = s_in_progress;
}

void Transaction::stop(bool committed, unsigned* writeset, unsigned nwriteset) {
#if STO_TSC_PROFILE
    TimeKeeper<tc_cleanup> tk;
#endif
    if (!committed) {
        TXP_INCREMENT(txp_total_aborts);
#if STO_DEBUG_ABORTS
        if (local_random() <= uint32_t(0xFFFFFFFF * STO_DEBUG_ABORTS_FRACTION)) {
            std::ostringstream buf;
            buf << "$" << (threadid_ < 10 ? "0" : "") << threadid_
                << " abort " << state_name(state_);
            if (abort_reason_)
                buf << " " << abort_reason_;
            if (abort_item_)
                buf << " " << *abort_item_;
            if (abort_version_)
                buf << " V" << TVersion(abort_version_);
            buf << '\n';
            std::cerr << buf.str();
        }
#endif
    }

    TXP_ACCOUNT(txp_max_transbuffer, buf_.buffer_size());
    TXP_ACCOUNT(txp_total_transbuffer, buf_.buffer_size());

    TransItem* it;
    if (!any_writes_)
        goto after_unlock;

    if (committed && !STO_SORT_WRITESET) {
        for (unsigned* idxit = writeset + nwriteset; idxit != writeset; ) {
            --idxit;
            if (*idxit < tset_initial_capacity)
                it = &tset0_[*idxit];
            else
                it = &tset_[*idxit / tset_chunk][*idxit % tset_chunk];
            if (it->needs_unlock())
                it->owner()->unlock(*it);
        }
        for (unsigned* idxit = writeset + nwriteset; idxit != writeset; ) {
            --idxit;
            if (*idxit < tset_initial_capacity)
                it = &tset0_[*idxit];
            else
                it = &tset_[*idxit / tset_chunk][*idxit % tset_chunk];
            if (it->has_write()) // always true unless a user turns it off in install()/check()
                it->owner()->cleanup(*it, committed);
        }
    } else {
        if (state_ == s_committing_locked) {
            it = &tset_[tset_size_ / tset_chunk][tset_size_ % tset_chunk];
            for (unsigned tidx = tset_size_; tidx != first_write_; --tidx) {
                it = (tidx % tset_chunk ? it - 1 : &tset_[(tidx - 1) / tset_chunk][tset_chunk - 1]);
                if (it->needs_unlock())
                    it->owner()->unlock(*it);
            }
        }
        it = &tset_[tset_size_ / tset_chunk][tset_size_ % tset_chunk];
        for (unsigned tidx = tset_size_; tidx != first_write_; --tidx) {
            it = (tidx % tset_chunk ? it - 1 : &tset_[(tidx - 1) / tset_chunk][tset_chunk - 1]);
            if (it->has_write())
                it->owner()->cleanup(*it, committed);
        }
    }

after_unlock:
    // TODO: this will probably mess up with nested transactions
    threadinfo_t& thr = tinfo[TThread::id()];
    if (thr.trans_end_callback)
        thr.trans_end_callback();
    // XXX should reset trans_end_callback after calling it...
    state_ = s_aborted + committed;

#if STO_TSC_PROFILE
    auto endtime = read_tsc();
    if (!committed)
        TSC_ACCOUNT(tc_abort, endtime - start_tsc_);
#endif
}

bool Transaction::try_commit() {
#if STO_TSC_PROFILE
    TimeKeeper<tc_commit> tk;
#endif
    assert(TThread::id() == threadid_);
#if ASSERT_TX_SIZE
    if (tset_size_ > TX_SIZE_LIMIT) {
        std::cerr << "transSet_ size at " << tset_size_
            << ", abort." << std::endl;
        assert(false);
    }
#endif
    TXP_ACCOUNT(txp_max_set, tset_size_);
    TXP_ACCOUNT(txp_total_n, tset_size_);

    assert(state_ == s_in_progress || state_ >= s_aborted);
    if (state_ >= s_aborted)
        return state_ > s_aborted;

    if (any_nonopaque_)
        TXP_INCREMENT(txp_commit_time_nonopaque);
#if !CONSISTENCY_CHECK
    // commit immediately if read-only transaction with opacity
    if (!any_writes_ && !any_nonopaque_) {
        stop(true, nullptr, 0);
        return true;
    }
#endif

    state_ = s_committing;

    unsigned writeset[tset_size_];
    unsigned nwriteset = 0;
    writeset[0] = tset_size_;

    TransItem* it = nullptr;
    for (unsigned tidx = 0; tidx != tset_size_; ++tidx) {
        it = (tidx % tset_chunk ? it + 1 : tset_[tidx / tset_chunk]);
        if (it->has_write()) {
            writeset[nwriteset++] = tidx;
#if !STO_SORT_WRITESET
            if (nwriteset == 1) {
                first_write_ = writeset[0];
                state_ = s_committing_locked;
            }
            if (!it->owner()->lock(*it, *this)) {
                mark_abort_because(it, "commit lock");
                goto abort;
            }
            it->__or_flags(TransItem::lock_bit);
#endif
        }
        if (it->has_read())
            TXP_INCREMENT(txp_total_r);
        else if (it->has_predicate()) {
            TXP_INCREMENT(txp_total_check_predicate);
            if (!it->owner()->check_predicate(*it, *this, true)) {
                mark_abort_because(it, "commit check_predicate");
                goto abort;
            }
        }
    }
    
    first_write_ = writeset[0];

    //phase1
#if STO_SORT_WRITESET
    std::sort(writeset, writeset + nwriteset, [&] (unsigned i, unsigned j) {
        TransItem* ti = &tset_[i / tset_chunk][i % tset_chunk];
        TransItem* tj = &tset_[j / tset_chunk][j % tset_chunk];
        return *ti < *tj;
    });

    if (nwriteset) {
        state_ = s_committing_locked;
        auto writeset_end = writeset + nwriteset;
        for (auto it = writeset; it != writeset_end; ) {
            TransItem* me = &tset_[*it / tset_chunk][*it % tset_chunk];
            if (!me->owner()->lock(*me, *this)) {
                mark_abort_because(me, "commit lock");
                goto abort;
            }
            me->__or_flags(TransItem::lock_bit);
            ++it;
        }
    }
#endif

#if CONSISTENCY_CHECK
    fence();
    commit_tid();
    fence();
#endif

    //phase2
    for (unsigned tidx = 0; tidx != tset_size_; ++tidx) {
        it = (tidx % tset_chunk ? it + 1 : tset_[tidx / tset_chunk]);
        if (it->has_read()) {
            TXP_INCREMENT(txp_total_check_read);
            if (!it->owner()->check(*it, *this)
                && (!may_duplicate_items_ || !preceding_duplicate_read(it))) {
                mark_abort_because(it, "commit check");
                goto abort;
            }
        }
    }

    // fence();

    //phase3
#if STO_SORT_WRITESET
    for (unsigned tidx = first_write_; tidx != tset_size_; ++tidx) {
        it = &tset_[tidx / tset_chunk][tidx % tset_chunk];
        if (it->has_write()) {
            TXP_INCREMENT(txp_total_w);
            it->owner()->install(*it, *this);
        }
    }
#else
    if (nwriteset) {
        auto writeset_end = writeset + nwriteset;
        for (auto idxit = writeset; idxit != writeset_end; ++idxit) {
            if (likely(*idxit < tset_initial_capacity))
                it = &tset0_[*idxit];
            else
                it = &tset_[*idxit / tset_chunk][*idxit % tset_chunk];
            TXP_INCREMENT(txp_total_w);
            it->owner()->install(*it, *this);
        }
    }
#endif

    // fence();
    stop(true, writeset, nwriteset);
    return true;

abort:
    // fence();
    TXP_INCREMENT(txp_commit_time_aborts);
    stop(false, nullptr, 0);
#if STO_TSC_PROFILE
    auto endtime = read_tsc();
    TSC_ACCOUNT(tc_commit_wasted, endtime - tk.init_tsc_val());
#endif
    return false;
}

/* CHOPPING */
bool Transaction::try_commit_piece(
        unsigned*& writeset, 
        void**& writekeys, void**& readkeys,
        unsigned& nwriteset, unsigned& nreadset) 
{
    assert(TThread::id() == threadid_);
#if ASSERT_TX_SIZE
    if (tset_size_ > TX_SIZE_LIMIT) {
        std::cerr << "transSet_ size at " << tset_size_
            << ", abort." << std::endl;
        assert(false);
    }
#endif
    assert(state_ == s_in_progress);
    
    if (any_nonopaque_)
        TXP_INCREMENT(txp_commit_time_nonopaque);

    size_t sz = tset_size_ - tset_piece_begin_;
    writeset = (unsigned*) malloc(sizeof(unsigned)*sz); 
    writekeys = (void**) malloc(sizeof(void*)*sz); 
    readkeys = (void**) malloc(sizeof(void*)*sz); 
    assert(writeset && writekeys && readkeys);
    nwriteset = nreadset = 0;
    writeset[0] = tset_size_;

    TransItem* it = nullptr;
    for (unsigned tidx = tset_piece_begin_; tidx != tset_size_; ++tidx) {
        it = ((tidx % tset_chunk && it) ? it + 1 : &tset_[tidx / tset_chunk][tidx % tset_chunk]);
        if (it->has_write()) {
            writekeys[nwriteset] = it->get_void_key();
            writeset[nwriteset++] = tidx;
#if !STO_SORT_WRITESET
            if (nwriteset == 1) {
                first_piece_write_ = writeset[0];
                state_ = s_committing_locked;
            }
            if (!it->owner()->lock(*it, *this)) {
                mark_abort_because(it, "commit lock");
                assert(0);
            }
            it->__or_flags(TransItem::lock_bit);
#endif
        }
        if (it->has_read() || it->has_predicate()) {
            TXP_INCREMENT(txp_total_r);
            readkeys[nreadset++] = it->get_void_key();
        } 
    }
    if (tset_piece_begin_ == 0) {
        // set the first write for aborting the entire txn
        first_write_ = writeset[0];
    }
    // this is to know the first write to commit for this piece
    first_piece_write_ = writeset[0];

    /* 
     * This is the same as the traditional STO commit protocol, except:
     * - we don't need to check (technically we don't need to lock either)
     * - we commit only those items from tset_piece_begin_ to tset_size_
     * - we return the state to s_in_progress after finishing the commit
     */

    //phase1
#if STO_SORT_WRITESET
    std::sort(writeset, writeset + nwriteset, [&] (unsigned i, unsigned j) {
        TransItem* ti = &tset_[i / tset_chunk][i % tset_chunk];
        TransItem* tj = &tset_[j / tset_chunk][j % tset_chunk];
        return *ti < *tj;
    });

    if (nwriteset) {
        state_ = s_committing_locked;
        auto writeset_end = writeset + nwriteset;
        for (auto it = writeset; it != writeset_end; ) {
            TransItem* me = &tset_[*it / tset_chunk][*it % tset_chunk];
            if (!me->owner()->lock(*me, *this)) {
                mark_abort_because(me, "commit lock");
                assert(0);
                goto abort;
            }
            me->__or_flags(TransItem::lock_bit);
            ++it;
        }
    }
#endif

    // fence();
    //phase3
#if STO_SORT_WRITESET
    for (unsigned tidx = first_piece_write_; tidx != tset_size_; ++tidx) {
        it = &tset_[tidx / tset_chunk][tidx % tset_chunk];
        if (it->has_write()) {
            TXP_INCREMENT(txp_total_w);
            it->owner()->install(*it, *this);
        }
    }
#else
    if (nwriteset) {
        auto writeset_end = writeset + nwriteset;
        for (auto idxit = writeset; idxit != writeset_end; ++idxit) {
            if (likely(*idxit < tset_initial_capacity)) {
                it = &tset0_[*idxit];
            }
            else
                it = &tset_[*idxit / tset_chunk][*idxit % tset_chunk];
            TXP_INCREMENT(txp_total_w);
            assert (it->has_write());
            it->owner()->install(*it, *this);
        }
    }
#endif

    // fence();
    // unlock and cleanup
    TXP_ACCOUNT(txp_max_transbuffer, buf_.buffer_size());
    TXP_ACCOUNT(txp_total_transbuffer, buf_.buffer_size());

    if (!any_writes_)
        goto after_unlock;

    if (!STO_SORT_WRITESET) {
        for (unsigned* idxit = writeset + nwriteset; idxit != writeset; ) {
            --idxit;
            if (*idxit < tset_initial_capacity)
                it = &tset0_[*idxit];
            else
                it = &tset_[*idxit / tset_chunk][*idxit % tset_chunk];
            if (it->needs_unlock())
                it->owner()->unlock(*it);
        }
        for (unsigned* idxit = writeset + nwriteset; idxit != writeset; ) {
            --idxit;
            if (*idxit < tset_initial_capacity)
                it = &tset0_[*idxit];
            else
                it = &tset_[*idxit / tset_chunk][*idxit % tset_chunk];
            if (it->has_write()) {// always true unless a user turns it off in install()/check()
                it->owner()->cleanup(*it, true);
                //it->__rm_flags(TransItem::write_bit);
            }
        } 
    } else {
        if (state_ == s_committing_locked) {
            it = &tset_[tset_size_ / tset_chunk][tset_size_ % tset_chunk];
            for (unsigned tidx = tset_size_; tidx != first_piece_write_; --tidx) {
                it = (tidx % tset_chunk ? it - 1 : &tset_[(tidx - 1) / tset_chunk][tset_chunk - 1]);
                if (it->needs_unlock())
                    it->owner()->unlock(*it);
            }
        }
        it = &tset_[tset_size_ / tset_chunk][tset_size_ % tset_chunk];
        for (unsigned tidx = tset_size_; tidx != first_piece_write_; --tidx) {
            it = (tidx % tset_chunk ? it - 1 : &tset_[(tidx - 1) / tset_chunk][tset_chunk - 1]);
            if (it->has_write()) {
                it->owner()->cleanup(*it, true);
                //it->__rm_flags(TransItem::write_bit);
            }
       }
    }

after_unlock:
    tset_piece_begin_ = tset_size_;
    state_ = s_in_progress;
    return true;
}
/* END CHOPPING */

void Transaction::print_stats() {
    txp_counters out = txp_counters_combined();
    //if (out.p(txp_overlap)) {
        //fprintf(stderr,"$ %llu wait end, %llu wait start, %llu wait invalid\n",
         //       out.p(txp_wait_end), out.p(txp_wait_start), out.p(txp_wait_invalid));
        fprintf(stderr,"%llu\t",//, %llu overlap invalid\n",
                out.p(txp_overlap));//, out.p(txp_overlap_invalid));
    //}
    if (txp_count >= txp_max_set) {
        unsigned long long txc_total_starts = out.p(txp_total_starts);
        unsigned long long txc_total_aborts = out.p(txp_total_aborts);
        unsigned long long txc_commit_aborts = out.p(txp_commit_time_aborts);
        unsigned long long txc_total_commits = txc_total_starts - txc_total_aborts;
        //fprintf(stderr, "$ %llu starts, %llu max read set, %llu commits",
         //       txc_total_starts, out.p(txp_max_set), txc_total_commits);
        //if (txc_total_aborts) {
            fprintf(stderr, "%.3f\t",
                    100.0 * (double) out.p(txp_total_aborts) / out.p(txp_total_starts));
            //if (out.p(txp_commit_time_aborts))
                //fprintf(stderr, "\n$ %llu (%.3f%%) of aborts at commit time",
                        //out.p(txp_commit_time_aborts),
                        //100.0 * (double) out.p(txp_commit_time_aborts) / out.p(txp_total_aborts));
        //}
        unsigned long long txc_commit_attempts = txc_total_starts - (txc_total_aborts - txc_commit_aborts);
        //fprintf(stderr, "\n$ %llu commit attempts, %llu (%.3f%%) nonopaque\n",
        //        txc_commit_attempts, out.p(txp_commit_time_nonopaque),
         //       100.0 * (double) out.p(txp_commit_time_nonopaque) / txc_commit_attempts);
    }
    /*if (txp_count >= txp_hco_abort)
        fprintf(stderr, "$ %llu HCO (%llu lock, %llu invalid, %llu aborts) out of %llu check attempts (%.3f%%)\n",
                out.p(txp_hco), out.p(txp_hco_lock), out.p(txp_hco_invalid), out.p(txp_hco_abort), out.p(txp_tco),
                100.0 * (double) out.p(txp_hco) / out.p(txp_tco));
    if (txp_count >= txp_hash_collision)
        fprintf(stderr, "$ %llu (%.3f%%) hash collisions, %llu second level\n", out.p(txp_hash_collision),
                100.0 * (double) out.p(txp_hash_collision) / out.p(txp_hash_find),
                out.p(txp_hash_collision2));
    if (txp_count >= txp_total_transbuffer)
        fprintf(stderr, "$ %llu max buffer per txn, %llu total buffer\n",
                out.p(txp_max_transbuffer), out.p(txp_total_transbuffer));
    fprintf(stderr, "$ %llu next commit-tid\n", (unsigned long long) _TID);*/

#if STO_TSC_PROFILE
    tc_counters out_tcs = tc_counters_combined();
    std::stringstream ss;
    ss << std::endl << "$ Timing breakdown: " << std::endl;
    ss << "   time_commit: " << out_tcs.to_realtime(tc_commit) << std::endl;
    ss << "   time_commit_wasted: " << out_tcs.to_realtime(tc_commit_wasted) << std::endl;
    ss << "   time_find_item: " << out_tcs.to_realtime(tc_find_item) << std::endl;
    ss << "   time_abort: " << out_tcs.to_realtime(tc_abort) << std::endl;
    ss << "   time_cleanup: " << out_tcs.to_realtime(tc_cleanup) << std::endl;
    ss << "   time_opacity: " << out_tcs.to_realtime(tc_opacity) << std::endl;

    fprintf(stderr, "%s\n", ss.str().c_str());
#endif
}

const char* Transaction::state_name(int state) {
    static const char* names[] = {"in-progress", "opacity-check", "committing", "committing-locked", "aborted", "committed"};
    if (unsigned(state) < arraysize(names))
        return names[state];
    else
        return "unknown-state";
}

void Transaction::print(std::ostream& w) const {
    w << "T0x" << (void*) this << " " << state_name(state_) << " [";
    const TransItem* it = nullptr;
    for (unsigned tidx = 0; tidx != tset_size_; ++tidx) {
        it = (tidx % tset_chunk ? it + 1 : tset_[tidx / tset_chunk]);
        if (tidx)
            w << " ";
        it->owner()->print(w, *it);
    }
    w << "]\n";
}

void Transaction::print() const {
    print(std::cerr);
}

void TObject::print(std::ostream& w, const TransItem& item) const {
    w << "{" << typeid(*this).name() << " " << (void*) this << "." << item.key<void*>();
    if (item.has_read())
        w << " R" << item.read_value<void*>();
    if (item.has_write())
        w << " =" << item.write_value<void*>();
    if (item.has_predicate())
        w << " P" << item.predicate_value<void*>();
    w << "}";
}

std::ostream& operator<<(std::ostream& w, const Transaction& txn) {
    txn.print(w);
    return w;
}

std::ostream& operator<<(std::ostream& w, const TestTransaction& txn) {
    txn.print(w);
    return w;
}

std::ostream& operator<<(std::ostream& w, const TransactionGuard& txn) {
    txn.print(w);
    return w;
}
