#include <iostream>
#include "series.h"

namespace py = pybind11;


py::array_t<double> shift_forward(py::array_t<unsigned long int> ts, py::array_t<double> xs, long int delta_us) {
    py::buffer_info ts_buf = ts.request();
    unsigned long int *ts_ptr = static_cast<unsigned long int *>(ts_buf.ptr);

    py::buffer_info xs_buf = xs.request();
    double *xs_ptr = static_cast<double *>(xs_buf.ptr);

    py::array_t<double> future_xs = py::array_t<double>(xs_buf.size);
    py::buffer_info future_xs_buf = future_xs.request();
    double *future_xs_ptr = static_cast<double *>(future_xs_buf.ptr);

    int j = 0;
    for(auto i=0; i<ts_buf.size; i++) {
        while ((j < ts_buf.size - 1) and (ts_ptr[j] - ts_ptr[i] < delta_us)) {
            j += 1;
        }
        future_xs_ptr[i] = xs_ptr[j];
    }
    return future_xs;
}

PYBIND11_MODULE(cseries, m) {
    m.doc() = "timeseries plugin";

    m.def("shift_forward", &shift_forward, "shift time series forward");
}
