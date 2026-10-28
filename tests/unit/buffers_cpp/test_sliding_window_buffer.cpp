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
    SlidingWindowBuffer buf(DataExtractionPolicy::ExtractTradeTickCQG, 1.5, 0.25,
                             StatConfig{true, false, false, false});

    py::object parsed_rtmd = make_parsed_rtmd({
        make_cqg_quote(1.0, 100.0, 9),
        make_cqg_quote(2.0, 114.0, 4),
        make_cqg_quote(3.0, 90.0, 11),
        make_cqg_quote(4.0, 103.0, 3),
    });

    buf.on_tick(parsed_rtmd);
    
    EXPECT_EQ(buf.size(), 2);

    py::tuple actual = buf.get_stat_snapshot(StatType::OHLCV).cast<py::tuple>();
    
    EXPECT_DOUBLE_EQ(actual[0].cast<double>(), 90.0);
    EXPECT_DOUBLE_EQ(actual[1].cast<double>(), 103.0);
    EXPECT_DOUBLE_EQ(actual[2].cast<double>(), 90.0);
    EXPECT_DOUBLE_EQ(actual[3].cast<double>(), 103.0);
    EXPECT_EQ(actual[4].cast<double>(), 14);
}
/*
TEST(SlidingWindowBuffer, GetSnapshotBatchHappyPath) {
    SlidingWindowBuffer buf(DataExtractionPolicy::ExtractTradeTickCQG, 10.0, 0.25,
                             StatConfig{true, true, true, false});
                             
    double price_arr[7] = {100,200,105,114,20,50,32};
    int vols_arr[7] = {10,46,4,81,5,42,23};
    
    py::object parsed_rtmd = make_parsed_rtmd({
        make_cqg_quote(1.0, price_arr[0], vols_arr[0]),
        make_cqg_quote(2.0, price_arr[1], vols_arr[1]),
        make_cqg_quote(3.0, price_arr[2], vols_arr[2]),
        make_cqg_quote(4.0, price_arr[3], vols_arr[3]),
        make_cqg_quote(5.0, price_arr[4], vols_arr[4]),
        make_cqg_quote(6.0, price_arr[5], vols_arr[5]),
        make_cqg_quote(7.0, price_arr[6], vols_arr[6]),
    });

    buf.on_tick(parsed_rtmd);
    
    py::dict actual = buf.get_stat_snapshot_batch();
    
    // Check OHLCV
    py::tuple actual_ohlcv = actual[py::cast(StatType::OHLCV)];
    EXPECT_NEAR(actual_ohlcv[0].cast<double>(), 100.0, 1e-4);
    EXPECT_NEAR(actual_ohlcv[1].cast<double>(), 200.0, 1e-4);
    EXPECT_NEAR(actual_ohlcv[2].cast<double>(), 20.0, 1e-4);
    EXPECT_NEAR(actual_ohlcv[3].cast<double>(), 32.0, 1e-4);
    EXPECT_EQ(actual_ohlcv[4].cast<double>(), 211);

    // Check Moment
    py::tuple actual_moment = actual[py::cast(StatType::MOMENT)];
    EXPECT_NEAR(actual_moment[0].cast<double>(), 88.7143, 1e-4);
    EXPECT_NEAR(actual_moment[1].cast<double>(), 3264.7755, 1e-4);
    EXPECT_NEAR(actual_moment[2].cast<double>(), 0.63964, 1e-4);
    EXPECT_NEAR(actual_moment[3].cast<double>(), 2.5299, 1e-4);

    // Check VWAP
    py::tuple actual_vwap = actual[py::cast(StatType::VWAP)];
    EXPECT_NEAR(actual_vwap[0].cast<double>(), 108.00947867, 1e-4);
}*/
