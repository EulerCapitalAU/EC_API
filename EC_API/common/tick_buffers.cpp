#include <deque>
#include <string>
#include <array>
#include <vector>
#include <Python.h>
#include <pybind11/pybind11.h>
#include "ticks.h"
#include "stats.h"
#include "data_fields_cqg.h"
#include "data_extractors_cqg.h"
#include "tick_buffers.h"
namespace py = pybind11;
//python setup.py build_ext --inplace

static TradeTick default_tick{0,0,0,};
//* -------------------- SldingWindowBuffer -------------------- */
// Constructor
SlidingWindowBuffer::SlidingWindowBuffer(
    DataExtractionPolicy policy, 
    double window, 
    double tick_size, 
    StatConfig stat_config
    ): 
    
    policy_{policy},
    rtmd_idx{get_parsed_rtmd_index_from_policy(policy)},
    window_{window},
    stat_config_{stat_config},
    stats_{} {
        init_stats(  
            stat_config_, 
            tick_container_,
            stats_, 
            active_stats_,
            tick_size
            );
    }
   
/*Properties method*/ 
int SlidingWindowBuffer::size() {return tick_container_.size();}

bool SlidingWindowBuffer::empty() {return tick_container_.empty();}

void SlidingWindowBuffer::reset() {tick_container_.clear();}

TradeTick& SlidingWindowBuffer::latest() {return tick_container_.back();}

/*Statistical Computation Methods*/
void SlidingWindowBuffer::compute_and_update(const TradeTick& tick) {//accumulator calcultator
    // Evict stat data given old ticks in the scratch buffer
    while (!scratch_tick_container_.empty()) {
        auto ev = scratch_tick_container_.back();
        scratch_tick_container_.pop_back();
        for (auto& sts : active_stats_) { // evict for all stat objects
            sts->evict(ev);
        }
    }         
    for (auto& sts : active_stats_) {// update for all stat objects
        sts->update(tick);
        }; 
}

/*Tick Advancement Methods*/
void SlidingWindowBuffer::add_tick(const TradeTick& tick) {
    tick_container_.push_back(tick);
    }

void SlidingWindowBuffer::evict_tick(const double threshold) { // loop through the front of the container
    while (!tick_container_.empty() && tick_container_.front().timestamp < threshold) {
        TradeTick evicted_tick = tick_container_.front();
        tick_container_.pop_front();
        scratch_tick_container_.push_back(evicted_tick);
        };
    }   

void SlidingWindowBuffer::on_tick(const py::object& parsed_rtmd) {
    if (!PyTuple_Check(parsed_rtmd.ptr())) return;
    
    const PyObject* data = PyTuple_GET_ITEM(parsed_rtmd.ptr(), rtmd_idx);

    if (!PyList_Check(data)) return;
    Py_ssize_t n = PyList_GET_SIZE(data);
    for (Py_ssize_t i = 0; i < n; i++) {
        PyObject* raw_data_ptr = PyList_GET_ITEM(data, i);
        if (!PyTuple_Check(raw_data_ptr)) continue;  //
        
        auto tick = extract_raw_tick(raw_data_ptr, policy_);
        
        if (tick) {
            double threshold_ = tick->timestamp - window_;
            
            add_tick(*tick); // fix this via timed eviction
            evict_tick(threshold_);
            compute_and_update(*tick);
            }
        }
}

/* Getter methods for statistics snapshots*/
py::object SlidingWindowBuffer::get_stat_snapshot(const StatType& stat_name) const {
    auto* ss = stats_[static_cast<size_t>(stat_name)];
    return ss ? ss->to_py_tuple() : py::none();
}

py::object SlidingWindowBuffer::get_stat_snapshot_batch() const {
    py::dict out; 
    for (size_t i = 0; i < stats_.size(); ++i)
        if (stats_[i])
            out[py::cast(static_cast<StatType>(i))] = stats_[i]->to_py_tuple();        
    return out;
}  
    
//* -------------------- RingBuffer -------------------- */
RingBuffer::RingBuffer(DataExtractionPolicy policy, int capacity):
    policy_{policy},
    rtmd_idx{get_parsed_rtmd_index_from_policy(policy)},
    capacity_{capacity} {
    }
    
/*Properties method*/ 
int RingBuffer::size() {return tick_container_.size();}

bool RingBuffer::isEmpty() {
    if (size_==0) {return true;}
    return false;
}

bool RingBuffer::isFull() {
    if (size_==capacity_) {return true;}
    return false;
}

void RingBuffer::reset() {
    tick_container_.fill(default_tick);
    head_ = 0; tail_ = 0; size_ = 0;
}

TradeTick& RingBuffer::latest() {
    if (isEmpty()){return default_tick;}
        return tick_container_[(tail_ - 1 + capacity_) % capacity_];
}

/*Statistical Computation Methods*/
void RingBuffer::compute_and_update(const TradeTick& tick) {}

/*Tick Advancement Methods*/
void RingBuffer::add_tick(const TradeTick& tick) {
    if (!isFull()){
        tick_container_[tail_] = tick;
        tail_ = (tail_ + 1) % capacity_;
        size_+=1;
    }

}

void RingBuffer::evict_tick(const double threshold) {}

void RingBuffer::on_tick(const py::object& parsed_rtmd) {}

/* Getter methods for statistics snapshots*/
py::object RingBuffer::get_stat_snapshot(const StatType& stat_name) const {
    auto* ss = stats_[static_cast<size_t>(stat_name)];
    return ss ? ss->to_py_tuple() : py::none();
}

py::object RingBuffer::get_stat_snapshot_batch() const {    
    py::dict out; 
    for (size_t i = 0; i < stats_.size(); ++i)
        if (stats_[i])
            out[py::cast(static_cast<StatType>(i))] = stats_[i]->to_py_tuple();        
    return out;}

