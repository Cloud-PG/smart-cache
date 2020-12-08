from distutils.core import setup

setup(
    name='probe',
    version='1.0.0',
    author='Mirco Tracolli',
    author_email='mirco.tracolli@pg.infn.it',
    packages=[
        'probe',
        'probe.analyzer',
        'probe.plotter',
        'probe.converter',
        'probe.qTable',
        'probe.results',
    ],
    scripts=[],
    url='https://github.com/Cloud-PG/smart-cache',
    license='Apache 2.0 License',
    description='The probe tool for the SmartCache project',
    long_description="To do...",
    install_requires=open("requirements.txt").read(),
    classifier=[
        "Operating System :: POSIX :: Linux",
        "License :: OSI Approved :: Apache 2.0 License",
        "Programming Language :: Python :: 3 :: Only"
    ]
)
