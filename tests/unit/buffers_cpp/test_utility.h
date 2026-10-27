#pragma once
#include <vector>
#include <pybind11/pybind11.h>
#include "EC_API/common/data_fields_cqg.h"
#include "EC_API/common/data_extractors_cqg.h"

namespace py = pybind11;

/*------- Test Utilities -------*/
inline py::tuple make_cqg_quote(double timestamp, double price, double volume) {
    return py::make_tuple(
        0,                                      // Q_CONTRACT_ID (unused)
        static_cast<int>(QuoteTypeCQG::Trade),   // Q_TYPE
        py::float_(timestamp),                   // Q_UTC_TIME
        py::float_(price),                        // Q_SCALED_PRICE
        py::none(), py::none(),                   // Q_SCALED_SOURCE_PRICE, Q_PRICE_YIELD
        py::float_(volume),                        // Q_VOL_SIGNIFICAND
        py::float_(0.0),                            // Q_VOL_EXPONENT
        py::none(), py::none(), py::none(), py::none(), py::none(), py::none(), // unused
        py::float_(1.0),                             // Q_CORRECT_PRICE_SCALE — 1.0 so price passes through unscaled
        py::none()                                    // Q_IS_SNAPSHOT
    );
}

inline py::tuple make_parsed_rtmd(std::vector<py::tuple> quotes) {
    py::list quote_list;
    for (auto& q : quotes) quote_list.append(q);
    return py::make_tuple(quote_list, py::none(), py::none(), py::none());  // [Quote, MarketValue, DetailedDOM, Corrections]
}
