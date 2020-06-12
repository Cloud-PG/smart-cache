from setuptools import setup
from distutils.extension import Extension
from Cython.Build import cythonize
import numpy

extensions = [
    Extension("cacheenvnewcython", sources=["cacheenvnewcython.pyx"], include_dirs=[numpy.get_include()], extra_compile_args=["-O3"], language="c++")
]

setup(
    ext_modules = cythonize(extensions)
)
