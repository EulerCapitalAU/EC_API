#include <pybind11/pybind11.h>
#include "ticks.h"
#include "stats.h"
#include "data_fields_cqg.h"
#include "data_extractors_cqg.h"
#include "tick_buffers.h"
namespace py = pybind11;


PYBIND11_MODULE(tick_buffers_ext, m) {
    py::enum_<StatType>(m, "StatType")
        .value("OHLCV", StatType::OHLCV)
        .value("VWAP", StatType::VWAP)
        .value("MOMENT", StatType::MOMENT)
        .value("MEDIAN", StatType::MEDIAN);

    py::enum_<DataExtractionPolicy>(m, "DataExtractionPolicy")
        .value("ExtractTradeTickCQG", DataExtractionPolicy::ExtractTradeTickCQG);
        
    py::class_<StatConfig>(m, "StatConfig")
        .def(py::init([](bool cal_ohlcv, bool cal_moment, bool cal_vwap, bool cal_median) {
                 return StatConfig{cal_ohlcv, cal_moment, cal_vwap, cal_median};
             }),
             py::arg("cal_ohlcv")  = false,
             py::arg("cal_moment") = false,
             py::arg("cal_vwap")   = false,
             py::arg("cal_median") = false);
           
    py::class_<SlidingWindowBuffer>(m, "SlidingWindowBuffer")
        .def(py::init<DataExtractionPolicy, double, double, StatConfig>(),
             py::arg("policy"),
             py::arg("window"),
             py::arg("tick_size"),
             py::arg("stat_config") = StatConfig{}) 
        .def("on_tick", &SlidingWindowBuffer::on_tick)
        .def("get_stat_snapshot", &SlidingWindowBuffer::get_stat_snapshot)
        .def("get_stat_snapshot_batch", &SlidingWindowBuffer::get_stat_snapshot_batch);
        
    py::class_<RingBuffer>(m, "RingBuffer")
        .def(py::init<DataExtractionPolicy, int>(),
             py::arg("policy"),
             py::arg("buffer_size")
             )
            
        .def("on_tick", &RingBuffer::on_tick);
}