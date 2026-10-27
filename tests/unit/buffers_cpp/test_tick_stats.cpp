#include <gtest/gtest.h>
#include <deque>
#include "EC_API/common/stats.h"
#include "EC_API/common/ticks.h"

/*---------------------------*/
//Happy Paths -- Deque init
//test OHLCV init 
TEST(OHLCVStat, InitSuccess) {
    std::deque<TradeTick> window;
    OHLCVStat<std::deque<TradeTick>> stat_obj(window, 0.01);

    OHLCVSnapshot snap = stat_obj.get_snapshot();
    EXPECT_DOUBLE_EQ(snap.open, 0.0);
    EXPECT_DOUBLE_EQ(snap.high, std::numeric_limits<double>::lowest());
    EXPECT_DOUBLE_EQ(snap.low, std::numeric_limits<double>::max());
    EXPECT_DOUBLE_EQ(snap.close, 0.0);
    EXPECT_EQ(snap.volume, 0);
}

//test OHLCV update
TEST(OHLCVStat, UpdateSucess) {
    //init
    std::deque<TradeTick> window;
    OHLCVStat<std::deque<TradeTick>> stat_obj(window, 0.01);
    
    //ingest data and update stat
    window.push_back({0.0,100.0,10});
    stat_obj.update(window.back());
    window.push_back({1.0,105.0,5});
    stat_obj.update(window.back());
    window.push_back({2.0,98.0,3});
    stat_obj.update(window.back());

    // tests
    OHLCVSnapshot snap = stat_obj.get_snapshot();
    EXPECT_DOUBLE_EQ(snap.open, 100.0);
    EXPECT_DOUBLE_EQ(snap.high, 105.0);
    EXPECT_DOUBLE_EQ(snap.low,98.0);
    EXPECT_DOUBLE_EQ(snap.close,98.0);
    EXPECT_EQ(snap.volume, 18);
}
//test OHLCV evict
TEST(OHLCVStat, EvictSuccess) {
    //init
    std::deque<TradeTick> window;
    OHLCVStat<std::deque<TradeTick>> stat_obj(window, 0.01);
    
    window.push_back({0.0,100.0,10});
    stat_obj.update(window.back());
    window.push_back({1.0,105.0,5});
    
    TradeTick evicted = window.front();
    window.pop_front();
    
    stat_obj.evict(evicted);
    stat_obj.update(window.back());
    
    OHLCVSnapshot snap = stat_obj.get_snapshot();
    EXPECT_DOUBLE_EQ(snap.open, 105.0);
    EXPECT_DOUBLE_EQ(snap.high, 105.0);
    EXPECT_DOUBLE_EQ(snap.low,105.0);
    EXPECT_DOUBLE_EQ(snap.close,105.0);
    EXPECT_EQ(snap.volume, 5);
}

// Sad Paths
//test OHLCV evict empty guard
TEST(OHLCVStat, EvictFailEmptyContainer) {
    //init
    std::deque<TradeTick> window;
    OHLCVStat<std::deque<TradeTick>> stat_obj(window, 0.01);
    
    TradeTick t = TradeTick({0.0,100.0,10});
    
    stat_obj.evict(t); // evict when the container is empty

    OHLCVSnapshot snap = stat_obj.get_snapshot();
    EXPECT_DOUBLE_EQ(snap.open, 0.0);
    EXPECT_DOUBLE_EQ(snap.high, std::numeric_limits<double>::lowest());
    EXPECT_DOUBLE_EQ(snap.low, std::numeric_limits<double>::max());
    EXPECT_DOUBLE_EQ(snap.close, 0.0);
    EXPECT_EQ(snap.volume, 0);
}

// Sad/edge path: evicting the current HIGH forces a rescan
TEST(OHLCVStat, EvictRescanHigh) {
    std::deque<TradeTick> window;
    OHLCVStat<std::deque<TradeTick>> stat_obj(window, 0.01);

    window.push_back({0.0,100.0,10}); stat_obj.update(window.back());
    window.push_back({1.0,105.0,5});  stat_obj.update(window.back()); // high=105
    window.push_back({2.0,102.0,3});  stat_obj.update(window.back()); // low=100

    TradeTick evicted = window[1];        // the 105.0 tick — current high
    window.erase(window.begin() + 1);
    stat_obj.evict(evicted);

    OHLCVSnapshot snap = stat_obj.get_snapshot();
    EXPECT_DOUBLE_EQ(snap.high, 102.0);   // rescanned max of {100, 102}
    EXPECT_DOUBLE_EQ(snap.low, 100.0);    // untouched: |105-100| > 0.5*tick_size_}
}
// Evicting the current LOW forces a rescan
TEST(OHLCVStat, EvictRescanLow) {
    std::deque<TradeTick> window;
    OHLCVStat<std::deque<TradeTick>> stat_obj(window, 0.01);

    window.push_back({0.0,100.0,10}); stat_obj.update(window.back());
    window.push_back({1.0,105.0,5});  stat_obj.update(window.back()); // high=105
    window.push_back({2.0,102.0,3});  stat_obj.update(window.back());
    
    TradeTick evicted = window.front();   // the 100.0 tick — current low
    window.pop_front();
    stat_obj.evict(evicted);

    OHLCVSnapshot snap = stat_obj.get_snapshot();
    EXPECT_DOUBLE_EQ(snap.low, 102.0);    // rescanned min of {105, 102}
    EXPECT_DOUBLE_EQ(snap.high, 105.0);   // untouched
}

// Neither branch should fire: evicting a tick that's neither high nor low
TEST(OHLCVStat, EvictNoRescanNeeded) {
    std::deque<TradeTick> window;
    OHLCVStat<std::deque<TradeTick>> stat_obj(window, 0.01);
    
    window.push_back({0.0,100.0,10}); stat_obj.update(window.back());
    window.push_back({1.0,105.0,5});  stat_obj.update(window.back());
    window.push_back({2.0,102.0,3});  stat_obj.update(window.back());
    
    TradeTick evicted = window[2];        // the 102.0 tick — neither high nor low
    window.erase(window.begin() + 2);
    stat_obj.evict(evicted);

    OHLCVSnapshot snap = stat_obj.get_snapshot();
    EXPECT_DOUBLE_EQ(snap.high, 105.0);   // unchanged
    EXPECT_DOUBLE_EQ(snap.low, 100.0);    // unchanged
}

/*---------------------------*/
//test MomentStat init 
TEST(MomentStat, InitSuccess) {
    std::deque<TradeTick> window;
    MomentStat<std::deque<TradeTick>> stat_obj(window);
            
    MomentSnapshot snap = stat_obj.get_snapshot();
    EXPECT_DOUBLE_EQ(snap.mean,0);
    EXPECT_DOUBLE_EQ(snap.variance,0);
    EXPECT_DOUBLE_EQ(snap.skewness,0);
    EXPECT_DOUBLE_EQ(snap.kurtosis,0);
}


TEST(MomentStat, NonEmptyContainerInitSuccess) {
    std::deque<TradeTick> window;
    
    double price_arr[7] = {100,200,105,114,20,50,32};
    double vols_arr[7] = {10,46,4,81,5,42,23};
    
    for (int i=0; i<7; i++) {
        window.push_back({static_cast<double>(i), price_arr[i], vols_arr[i]});
    } 
    
    // init when the window is not empty
    MomentStat<std::deque<TradeTick>> stat_obj(window);
    // it should contain the same result as in the UpdateSuccess test 
    MomentSnapshot snap = stat_obj.get_snapshot();
    EXPECT_NEAR(snap.mean, 88.7143, 1e-4);
    EXPECT_NEAR(snap.variance, 3264.7755, 1e-4);
    EXPECT_NEAR(snap.skewness,0.63964, 1e-4);
    EXPECT_NEAR(snap.kurtosis, 2.5299, 1e-4);
}

//test MomentStat update
TEST(MomentStat, UpdateSuccess) {
    std::deque<TradeTick> window;
    MomentStat<std::deque<TradeTick>> stat_obj(window);
    
    double price_arr[7] = {100,200,105,114,20,50,32};
    double vols_arr[7] = {10,46,4,81,5,42,23};
    
    for (int i=0; i<7; i++) {
        window.push_back({static_cast<double>(i), price_arr[i], vols_arr[i]});
        stat_obj.update(window.back());
    } 
    
    MomentSnapshot snap = stat_obj.get_snapshot();
    EXPECT_NEAR(snap.mean, 88.7143, 1e-4);
    EXPECT_NEAR(snap.variance, 3264.7755, 1e-4);
    EXPECT_NEAR(snap.skewness,0.63964, 1e-4);
    EXPECT_NEAR(snap.kurtosis, 2.5299, 1e-4);
}

//test MomentStat evict
TEST(MomentStat, EvictSuccess) {
    std::deque<TradeTick> window;
    MomentStat<std::deque<TradeTick>> stat_obj(window);
    
    double price_arr[7] = {100,200,105,114,20,50,32};
    double vols_arr[7] = {10,46,4,81,5,42,23};
    
    for (int i=0; i<7; i++) {
        window.push_back({static_cast<double>(i), price_arr[i], vols_arr[i]});
        stat_obj.update(window.back());
    } 

    TradeTick evicted = window.front();
    window.pop_front();
    
    stat_obj.evict(evicted); //evict the front and check new stat
        
    MomentSnapshot snap = stat_obj.get_snapshot();

    EXPECT_NEAR(snap.mean, 86.8334, 1e-4);
    EXPECT_NEAR(snap.variance, 3784.1389, 1e-4);
    EXPECT_NEAR(snap.skewness,0.6887, 1e-4);
    EXPECT_NEAR(snap.kurtosis, 2.2754, 1e-4);
}


//test VWAPStat init 
TEST(VWAPStat, InitSuccess) {
    std::deque<TradeTick> window;
    VWAPStat<std::deque<TradeTick>> stat_obj(window);
    
    VWAPSnapshot snap = stat_obj.get_snapshot();
    EXPECT_DOUBLE_EQ(snap.vwap,0);
}

TEST(VWAPStat, NonEmptyContainerInitSuccess) {
    std::deque<TradeTick> window;
    
    window.push_back({0.0,100.0,10});
    window.push_back({1.0,105.0,5});
    window.push_back({2.0,98.0,3});
    
    VWAPStat<std::deque<TradeTick>> stat_obj(window);
    
    VWAPSnapshot snap = stat_obj.get_snapshot();
    EXPECT_NEAR(snap.vwap, 101.0556, 1e-4);
}
//test VWAPStat update
TEST(VWAPStat, UpdateSuccess) {
    std::deque<TradeTick> window;
    VWAPStat<std::deque<TradeTick>> stat_obj(window);
    
    //ingest data and update stat
    window.push_back({0.0,100.0,10});
    stat_obj.update(window.back());
    window.push_back({1.0,105.0,5});
    stat_obj.update(window.back());
    window.push_back({2.0,98.0,3});
    stat_obj.update(window.back());

    VWAPSnapshot snap = stat_obj.get_snapshot();
    EXPECT_NEAR(snap.vwap, 101.0556, 1e-4);
}

//test VWAPStat evict
TEST(VWAPStat, EvictSuccess) {
    std::deque<TradeTick> window;
    VWAPStat<std::deque<TradeTick>> stat_obj(window);
    
    //ingest data and update stat
    window.push_back({0.0,100.0,10});
    stat_obj.update(window.back());
    window.push_back({1.0,105.0,5});
    stat_obj.update(window.back());
    window.push_back({2.0,98.0,3});
    stat_obj.update(window.back());
    
    TradeTick evicted = window.front();
    window.pop_front();
    
    stat_obj.evict(evicted); //evict the front and check new stat
    
    VWAPSnapshot snap = stat_obj.get_snapshot();
    EXPECT_NEAR(snap.vwap, 102.3750, 1e-4);
}

// test VWAP Sad Path 
TEST(VWAPStat, VWAPZeroDivisionFail) {
    std::deque<TradeTick> window;
    VWAPStat<std::deque<TradeTick>> stat_obj(window);

    window.push_back({0.0,0.0,0});
    
    stat_obj.update(window.back());
    VWAPSnapshot snap = stat_obj.get_snapshot();
    EXPECT_TRUE(std::isnan(snap.vwap));
}



