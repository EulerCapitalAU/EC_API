#include <gtest/gtest.h>
#include <deque>
#include <pybind11/embed.h>
#include "EC_API/common/stats.h"
#include "EC_API/common/data_extractors_cqg.h"
#include "EC_API/common/tick_buffers.h"
namespace py = pybind11;

static py::scoped_interpreter guard{}; 

TEST(SlidingWindowBuffer, InitStatSuccessAllStatSelected) {
    std::deque<TradeTick> window;
    SlidingWindowBuffer buf = SlidingWindowBuffer(
        DataExtractionPolicy::ExtractTradeTickCQG,
        10.0, 0.25,
        StatConfig{true, true, true, true}//OHLCV, moments, VWAP, Median
    );
    ASSERT_FALSE(buf.get_stat_snapshot(StatType::OHLCV).is_none());
    ASSERT_FALSE(buf.get_stat_snapshot(StatType::MOMENT).is_none());
    ASSERT_FALSE(buf.get_stat_snapshot(StatType::VWAP).is_none());
    ASSERT_FALSE(buf.get_stat_snapshot(StatType::MEDIAN).is_none());

}

TEST(SlidingWindowBuffer, InitStatSuccessNoStatSelected) {
    std::deque<TradeTick> window;

    SlidingWindowBuffer buf = SlidingWindowBuffer(
        DataExtractionPolicy::ExtractTradeTickCQG,
        10.0 /*micro-seconds*/, 0.25,
        StatConfig{false, false, false, false} //OHLCV, moments, VWAP, Median
    );
    ASSERT_TRUE(buf.get_stat_snapshot(StatType::OHLCV).is_none());
    ASSERT_TRUE(buf.get_stat_snapshot(StatType::MOMENT).is_none());
    ASSERT_TRUE(buf.get_stat_snapshot(StatType::VWAP).is_none());
    ASSERT_TRUE(buf.get_stat_snapshot(StatType::MEDIAN).is_none());
}