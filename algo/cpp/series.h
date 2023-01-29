#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

namespace py = pybind11;

namespace cseries {
    py::array_t<double> shift(py::array_t<unsigned long int> ts, py::array_t<double> xs, long int delta_us);
}



PYBIND11_MODULE(shift, m) {
    m.doc() = "timeseries plugin";

    m.def("shift", &cseries::shift, "shift time series forward");
}
