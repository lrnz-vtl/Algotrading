#pragma once

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

namespace py = pybind11;

py::array_t<double> shift_forward(py::array_t<unsigned long int> ts, py::array_t<double> xs, long int delta_us);
