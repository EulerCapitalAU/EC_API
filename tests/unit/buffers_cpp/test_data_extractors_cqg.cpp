#include <optional>
#include <gtest/gtest.h>
#include <Python.h>
#include <pybind11/pybind11.h>
#include <pybind11/embed.h>
#include "EC_API/common/data_fields_cqg.h"
#include "EC_API/common/data_extractors_cqg.h"
namespace py = pybind11;

// ---- make_float ----
TEST(MakeFloat, HappyPath) {
    // 5.0 * 10^0, positive
    EXPECT_DOUBLE_EQ(make_float(5.0, 0.0, 0), 5.0);
}

TEST(MakeFloat, PositiveExponentScalesUp) {
    // 1.5 * 10^2 = 150.0
    EXPECT_DOUBLE_EQ(make_float(1.5, 2.0, 0), 150.0);
}

TEST(MakeFloat, NegativeExponentScalesDown) {
    // 25.0 * 10^-1 = 2.5
    EXPECT_DOUBLE_EQ(make_float(25.0, -1.0, 0), 2.5);
}

TEST(MakeFloat, SignOneIsNegative) {
    // sign == 1 -> negated
    EXPECT_DOUBLE_EQ(make_float(5.0, 0.0, 1), -5.0);
}

TEST(MakeFloat, ZeroSignificandIsZeroRegardlessOfSign) {
    EXPECT_DOUBLE_EQ(make_float(0.0, 3.0, 1), 0.0);
}

TEST(MakeFloat, NonCanonicalSignIsTreatedAsNegative) {
    // documents actual behavior: the ternary is `sign == 0 ? +1 : -1`,
    // so ANY nonzero sign (not just 1) flips to negative — e.g. sign=2 here.
    EXPECT_DOUBLE_EQ(make_float(5.0, 0.0, 2), -5.0);
}


// --- get_parsed_rtmd_index_from_policy ---
TEST(GetParsedRTMDIndexFromPolicy, Pt2ExtractTradeTickCQG) {
    int res = get_parsed_rtmd_index_from_policy(DataExtractionPolicy::ExtractTradeTickCQG);
    EXPECT_EQ(res,ParsedRTMDCQG::Quote);
}

TEST(GetParsedRTMDIndexFromPolicy, Pt2Default) {
    auto unhandled = static_cast<DataExtractionPolicy>(255);
    EXPECT_THROW(get_parsed_rtmd_index_from_policy(unhandled), std::invalid_argument);
}

// extract_trade_tick_CQG
TEST(ExtractTradeTickCQG, HappyPath) {
    py::tuple quote = py::make_tuple(
        0,                                        // Q_CONTRACT_ID (unused by this fn)
        static_cast<int>(QuoteTypeCQG::Trade),    // Q_TYPE
        py::float_(1700000000.0),                 // Q_UTC_TIME
        py::float_(1050.0),                       // Q_SCALED_PRICE
        py::none(), py::none(),                   // Q_SCALED_SOURCE_PRICE, Q_PRICE_YIELD (unused)
        py::float_(5.0),                          // Q_VOL_SIGNIFICAND
        py::float_(0.0),                          // Q_VOL_EXPONENT
        py::none(), py::none(), py::none(), py::none(), py::none(), py::none(), // unused
        py::float_(0.01),                         // Q_CORRECT_PRICE_SCALE
        py::none()                                // Q_IS_SNAPSHOT
    );
    auto result = extract_trade_tick_CQG(quote.ptr());
    ASSERT_TRUE(result.has_value());
    EXPECT_DOUBLE_EQ(result->price, 1050.0 * 0.01);
}

TEST(ExtractTradeTickCQG, SadPathTimestampNotFloat) {
    py::tuple quote = py::make_tuple(
        0, static_cast<int>(QuoteTypeCQG::Trade),
        py::none(),               // Q_UTC_TIME — not a float, everything else valid
        py::float_(1050.0), py::none(), py::none(),
        py::float_(5.0), py::float_(0.0),
        py::none(), py::none(), py::none(), py::none(), py::none(), py::none(),
        py::float_(0.01), py::none()
    );
    EXPECT_FALSE(extract_trade_tick_CQG(quote.ptr()).has_value());
}

// extract_raw_tick
TEST(ExtractRawTick, Pt2ExtractTradeTickCQG) {
    py::tuple quote = py::make_tuple(
        0,                                        // Q_CONTRACT_ID (unused by this fn)
        static_cast<int>(QuoteTypeCQG::Trade),    // Q_TYPE
        py::float_(1700000000.0),                 // Q_UTC_TIME
        py::float_(1050.0),                       // Q_SCALED_PRICE
        py::none(), py::none(),                   // Q_SCALED_SOURCE_PRICE, Q_PRICE_YIELD (unused)
        py::float_(5.0),                          // Q_VOL_SIGNIFICAND
        py::float_(0.0),                          // Q_VOL_EXPONENT
        py::none(), py::none(), py::none(), py::none(), py::none(), py::none(), // unused
        py::float_(0.01),                         // Q_CORRECT_PRICE_SCALE
        py::none()                                // Q_IS_SNAPSHOT
    );
    
    auto res = extract_raw_tick(quote.ptr(), DataExtractionPolicy::ExtractTradeTickCQG);
    ASSERT_TRUE(res.has_value());
    EXPECT_DOUBLE_EQ(res->price, 1050.0 * 0.01);
}


TEST(ExtractRawTick, Pt2Default) {
    py::tuple quote = py::make_tuple(
        0,                                        // Q_CONTRACT_ID (unused by this fn)
        static_cast<int>(QuoteTypeCQG::Trade),    // Q_TYPE
        py::float_(1700000000.0),                 // Q_UTC_TIME
        py::float_(1050.0),                       // Q_SCALED_PRICE
        py::none(), py::none(),                   // Q_SCALED_SOURCE_PRICE, Q_PRICE_YIELD (unused)
        py::float_(5.0),                          // Q_VOL_SIGNIFICAND
        py::float_(0.0),                          // Q_VOL_EXPONENT
        py::none(), py::none(), py::none(), py::none(), py::none(), py::none(), // unused
        py::float_(0.01),                         // Q_CORRECT_PRICE_SCALE
        py::none()                                // Q_IS_SNAPSHOT
    );
    
    auto unhandled = static_cast<DataExtractionPolicy>(255);
    auto res = extract_raw_tick(quote.ptr(), unhandled);
    EXPECT_FALSE(res.has_value());
}



static py::scoped_interpreter guard{};


