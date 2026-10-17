#include <gtest/gtest.h>
#include <Python.h>
#include <pybind11/pybind11.h>
#include <pybind11/embed.h>
#include "EC_API/common/data_fields_cqg.h"
#include "EC_API/common/data_extractors_cqg.h"
namespace py = pybind11;

// make_float
TEST(MakeFloat, HappyPath) {}

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

// extract_raw_tick
TEST(ExtractRawTick, Pt2ExtractTradeTickCQG) {}

static py::scoped_interpreter guard{};


