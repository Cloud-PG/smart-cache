from distutils.core import setup

setup(
    name='SmartCacheTools',
    version='0.0.0',
    author='Mirco Tracolli',
    author_email='mirco.tracolli@pg.infn.it',
    packages=['DataManager', 'SmartCache'],
    scripts=[],
    url='https://github.com/Cloud-PG/smart-cache',
    license='Apache 2.0 License',
    description='Tool collection for SmartCache.',
    long_description="To do...",
    install_requires=open("requirements.txt").read(),
    classifier=[
        "Operating System :: POSIX :: Linux",
        "License :: OSI Approved :: Apache 2.0 License",
        "Programming Language :: Python :: 3 :: Only"
    ]
)
