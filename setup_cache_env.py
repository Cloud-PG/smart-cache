from distutils.extension import Extension

import numpy
from Cython.Build import cythonize
from setuptools import setup

extensions = [
    Extension(
        "cacheenvnewcython",
        sources=["cacheenvnewcython.pyx"],
        include_dirs=[
            numpy.get_include()
        ],
        extra_compile_args=["-O3"],
        language="c++"
    )
]

setup(
    ext_modules=cythonize(extensions)
)
