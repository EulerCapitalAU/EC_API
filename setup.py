from setuptools import setup
from pybind11.setup_helpers import Pybind11Extension, build_ext

ext_modules = [
    Pybind11Extension(
        "EC_API.common.tick_buffers_ext",
        sources=[
            "EC_API/common/tick_buffers_ext.cpp",
            "EC_API/common/tick_buffers.cpp",
            "EC_API/common/data_extractors_cqg.cpp",
            ],
        include_dirs=["EC_API/common"], 
        cxx_std=20,                
    ),
]

setup(
    ext_modules=ext_modules,
    cmdclass={"build_ext": build_ext},
)