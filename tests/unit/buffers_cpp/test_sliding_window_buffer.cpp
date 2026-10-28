#include <gtest/gtest.h>
#include <deque>
#include <array>
#include <pybind11/embed.h>
#include "EC_API/common/stats.h"
#include "EC_API/common/data_extractors_cqg.h"
#include "EC_API/common/tick_buffers.h"
#include "test_utility.h"
namespace py = pybind11;

static py::scoped_interpreter guard{}; 

/*------- SLidingwindowBuffer -------*/
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
/* Tick Lifecycle */
TEST(SlidingWindowBuffer, TickLifeCycle) {
    SlidingWindowBuffer buf = SlidingWindowBuffer(
        DataExtractionPolicy::ExtractTradeTickCQG,
        10.0 /*micro-seconds*/, 0.25,
        StatConfig{true, false, false, false} //OHLCV, moments, VWAP, Median
    );
    
    std::array<TradeTick, 4> tick_list{
        TradeTick{1.0, 100.0, 9},
        TradeTick{2.0, 114.0, 4},
        TradeTick{3.0, 90.0, 5},
        TradeTick{4.0, 111.0, 7}
    };

    // adding ticks
    for (auto& t : tick_list) {
        buf.add_tick(t);
    }

    TradeTick check_tick = TradeTick{4.0,111.0,7};

    EXPECT_EQ(buf.size(),4);
    EXPECT_EQ(buf.latest(), check_tick);
    
    // test eviction
    buf.evict_tick(3.0);
    EXPECT_EQ(buf.size(),2);

    //compute_and_update(*tick);
    std::array<OHLCVSnapshot, 2> snap_list{
        OHLCVSnapshot{90.0,90.0,90.0,90.0,5},
        OHLCVSnapshot{90.0,111.0,90.0,111.0,12}
    };
    
    for (int i=2; i<buf.size(); i++) {
        buf.compute_and_update(tick_list[i]);
        
        OHLCVSnapshot& check_ohlcv_snapshot = snap_list[i];
        
        py::tuple actual = buf.get_stat_snapshot(StatType::OHLCV).cast<py::tuple>();
        
        EXPECT_DOUBLE_EQ(actual[0].cast<double>(), check_ohlcv_snapshot.open);
        EXPECT_DOUBLE_EQ(actual[1].cast<double>(), check_ohlcv_snapshot.high);
        EXPECT_DOUBLE_EQ(actual[2].cast<double>(), check_ohlcv_snapshot.low);
        EXPECT_DOUBLE_EQ(actual[3].cast<double>(), check_ohlcv_snapshot.close);
        EXPECT_EQ(actual[4].cast<int>(), check_ohlcv_snapshot.volume);
    }
}

TEST(SlidingWindowBuffer, OnTickHappyPath) {
    SlidingWindowBuffer buf(DataExtractionPolicy::ExtractTradeTickCQG, 10.0, 0.25,
                             StatConfig{true, false, false, false});

    py::object parsed_rtmd = make_parsed_rtmd({
        make_cqg_quote(1.0, 100.0, 9),
        make_cqg_quote(2.0, 114.0, 4),
    });

    buf.on_tick(parsed_rtmd);

    EXPECT_EQ(buf.size(), 2);
}

