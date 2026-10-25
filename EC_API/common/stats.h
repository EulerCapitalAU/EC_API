#pragma once 
#include <cstdint>
#include <memory>
#include <deque>
#include <array>
#include <limits>
#include <vector>
#include <queue>
#include <pybind11/pybind11.h>
#include "ticks.h"
namespace py = pybind11;

enum class StatType { 
    OHLCV, 
    MOMENT, 
    VWAP, 
    MEDIAN, 
    COUNT_ 
};

inline constexpr size_t kStatTypeCount = static_cast<size_t>(StatType::COUNT_);

struct StatConfig {
    const bool cal_ohlcv = false;
    const bool cal_moment = false;
    const bool cal_vwap = false;
    const bool cal_median = false;
};

/* Snapshots collections */
/* DataFeed SnapShots */
struct OHLCVSnapshot {
    double open = 0; 
    double high = std::numeric_limits<double>::lowest();
    double low = std::numeric_limits<double>::max();
    double close = 0;
    int volume = 0;
};

struct MomentSnapshot {
    double mean = 0, variance = 0, skewness=0, kurtosis=0;
};

struct VWAPSnapshot {
    double vwap = 0;
};

struct MedianSnapshot {
    double median = 0;
};
/* CrossFeed SnapShots */
struct GreeksSnapshot {};


/* Stat classes for Stat in DataFeed*/
class StatBase {
public:
    virtual void update(const TradeTick& t)=0;
    virtual void evict(const TradeTick& t)=0;
    virtual py::object to_py_tuple() const {return py::none();}
    virtual ~StatBase() {};

};

// OHLCV, update O(1), evict O(N) worst case
template <typename ContainerT>
class OHLCVStat : public StatBase {
private:
    ContainerT* container_;
    double tick_size_; // price precision
    OHLCVSnapshot ohlcv_snapshot;
public:      
    OHLCVStat(ContainerT& buf, double tick_size);
    void update(const TradeTick& t) override;
    void evict(const TradeTick& t) override;
    OHLCVSnapshot get_snapshot() const;
    py::object to_py_tuple() const override;
};

// 1st-4nd order moments normalised
template <typename ContainerT>
class MomentStat : public StatBase {
private:
    ContainerT* container_;
    double sum_p;
    double sum_p_2;
    double sum_p_3;
    double sum_p_4;
    int count;
    MomentSnapshot moment_snapshot;
public:
    MomentStat(ContainerT& buf);
    void update(const TradeTick& t) override;
    void evict(const TradeTick& t) override;
    MomentSnapshot get_snapshot() const;
    py::object to_py_tuple() const override;
};

// 
template <typename ContainerT>
class VWAPStat : public StatBase {
private:
    ContainerT* container_;
    double sum_pv;
    double sum_v;
    VWAPSnapshot vwap_snapshot;
public:
    VWAPStat(ContainerT& buf);
    void update(const TradeTick& t) override;
    void evict(const TradeTick& t) override;
    VWAPSnapshot get_snapshot() const;
    py::object to_py_tuple() const override;
};

template <typename ContainerT>
class MedianStat : public StatBase {
private: 
    ContainerT container_;
    std::priority_queue<double> maxHeap;
    std::priority_queue<double> minHeap;
    double median;
public:
    MedianStat(ContainerT& buf);
    void update(const TradeTick& t) override;
    void evict(const TradeTick& t) override;

};

/* Stat classes for Stat in CrossFeed*/

/* setup functions */
template<typename C>
void init_stats(
    StatConfig& config, 
    C& tick_container,
    std::array<StatBase*, kStatTypeCount>& stats_array, 
    std::vector<std::unique_ptr<StatBase>>& active_stats,
    double tick_size_
    );


#include "stats.tpp"