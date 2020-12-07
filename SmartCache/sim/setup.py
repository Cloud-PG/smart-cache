from distutils.core import setup

setup(
    name='utils',
    version='1.0.0',
    author='Mirco Tracolli',
    author_email='mirco.tracolli@pg.infn.it',
    packages=[
        'utils',
    ],
    scripts=[],
    url='https://github.com/Cloud-PG/smart-cache',
    license='Apache 2.0 License',
    description='Utils for the SmartCache project',
    long_description="To do...",
    install_requires=open("requirements.txt").read(),
    classifier=[
        "Operating System :: POSIX :: Linux",
        "License :: OSI Approved :: Apache 2.0 License",
        "Programming Language :: Python :: 3 :: Only"
    ]
)
