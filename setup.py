from setuptools import setup, Extension
import numpy

from Cython.Build import cythonize


extensions = [
    Extension(
        name="stats",
        sources=[
            "dQL/stats.pyx"
        ],
        include_dirs=[
            numpy.get_include(),
        ],
        extra_compile_args=["-O3"],
        language="c++",
        
    )
]

setup(
    ext_modules=cythonize(
        extensions,
        language_level=3,
    ),
    package_dir={'': 'dQL'},
)
