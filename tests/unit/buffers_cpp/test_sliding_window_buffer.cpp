#include <gtest/gtest.h>
#include <deque>
#include <pybind11/embed.h>
#include "EC_API/common/stats.h"
#include "EC_API/common/data_extractors_cqg.h"
#include "EC_API/common/tick_buffers.h"
namespace py = pybind11;

static py::scoped_interpreter guard{}; 

/**/
TEST(SlidingWindowBuffer, InitStatSuccessAllStatSelected) {

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

/* Properties methods */
TEST(SlidingWindowBuffer, CheckSProperties) {
    SlidingWindowBuffer buf = SlidingWindowBuffer(
        DataExtractionPolicy::ExtractTradeTickCQG,
        10.0 /*micro-seconds*/, 0.25,
        StatConfig{false, true, false, true} //OHLCV, moments, VWAP, Median
    );
    buf.add_tick({1.0,100.0,9});
    buf.add_tick({2.0,114.0,4});
    
    EXPECT_EQ(buf.size(),2);
    ASSERT_FALSE(buf.empty());
    
    TradeTick check_tick = TradeTick{2.0,114.0,4};
    
    TradeTick& latest = buf.latest();
    EXPECT_EQ(latest, check_tick);
    buf.reset();
    ASSERT_TRUE(buf.empty());
}


