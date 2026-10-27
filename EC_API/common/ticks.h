#pragma once 
#include <variant>


struct TradeTick {
    double timestamp, price, volume;
    
    bool operator==(const TradeTick&) const = default;
    };

struct SpreadTick {
    double timestamp, price_a, price_b, spread;
    
    bool operator==(const SpreadTick&) const = default;
    };

struct OHLCVTick {
    double timestamp, open, high, low, close, volume;
    bool operator==(const OHLCVTick&) const = default;
    };

/*struct BidAskTick {
  double timestamp,      //← Q_UTC_TIME
  double bid_price,      //← Q_SCALED_PRICE * Q_CORRECT_PRICE_SCALE  (when Q_TYPE is bid)
  double ask_price,      //← Q_SCALED_PRICE * Q_CORRECT_PRICE_SCALE  (when Q_TYPE is ask)
  double bid_size,       //← volume from significand/exponent         (when Q_TYPE is bid)
  double ask_size       //← volume from significand/exponent         (when Q_TYPE is ask)
  };*/
  
using Tick = std::variant<TradeTick, OHLCVTick, SpreadTick>;


