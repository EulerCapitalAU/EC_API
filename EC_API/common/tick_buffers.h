#pragma once
#include <memory>
#include <array>
#include <vector>
#include <deque>
#include <pybind11/pybind11.h>
#include "ticks.h"
#include "stats.h"
#include "data_extractors_cqg.h"

namespace py = pybind11;

class SlidingWindowBuffer {
private:
    // Buffer setups
    DataExtractionPolicy policy_;
    int rtmd_idx; // The index of interest in rtmd tuple, initialised by policy_
    double window_; // time window. (micro-seconds)
    std::deque<TradeTick> tick_container_; // Tick Data Container
    std::vector<TradeTick> scratch_tick_container_; // temporary eviction room 
        
    // Stats
    StatConfig stat_config_; // A list of bools, input via Python
    std::array<StatBase*, static_cast<int>(StatType::COUNT_)> stats_; // StatBase Child objects all stat, for easy lookup
    std::vector<std::unique_ptr<StatBase>> active_stats_; // Active Stats only, for the update loop
    
public:
    SlidingWindowBuffer(
        DataExtractionPolicy policy, 
        double window, 
        double tick_size, 
        StatConfig stat_config = {}
        );
        
    /* Disable Move and Copy*/    
    SlidingWindowBuffer(const SlidingWindowBuffer&)              = delete;
    SlidingWindowBuffer& operator=(const SlidingWindowBuffer&)   = delete;
    SlidingWindowBuffer(const SlidingWindowBuffer&&)             = delete;
    SlidingWindowBuffer& operator=(SlidingWindowBuffer&&)       = delete;

    int size();
    bool empty();
    void reset();
    TradeTick& latest();
    
    void compute_and_update(const TradeTick& tick);
    void add_tick(const TradeTick& tick);
    void evict_tick(const double threshold);
    void on_tick(const py::object& parsed_rtmd);
    
    py::object get_stat_snapshot(const StatType& stat_name) const;
    py::object get_stat_snapshot_batch() const;

};


/* Fixed-Number entry Tick Buffer */
class RingBuffer {
private:
    // Buffer Setups
    DataExtractionPolicy policy_;
    int rtmd_idx;
    const int buffer_size_;
    std::vector<TradeTick> tick_container_;  // fixed size, no heap management needed
    int head_ = 0;
    
    // Stats
    StatConfig stat_config; 
    std::vector<StatBase*> stats_;

public:
    RingBuffer(DataExtractionPolicy policy, int buffer_size);

    /* Disable Move and Copy*/    
    RingBuffer(const RingBuffer&)              = delete;
    RingBuffer& operator=(const RingBuffer&)   = delete;
    RingBuffer(const RingBuffer&&)             = delete;
    RingBuffer& operator=(RingBuffer&&)       = delete;

    int size();
    bool empty();
    void reset();
    TradeTick& latest();
    
    void compute_and_update(const TradeTick& tick);
    void add_tick(const TradeTick& tick);
    void evict_tick(const double threshold);
    void on_tick(const py::object& parsed_rtmd);
    
    py::object get_stat_snapshot(const StatType& stat_name) const;
    py::object get_stat_snapshot_batch() const;
};