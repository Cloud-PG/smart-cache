from distutils.core import setup

setup(
    name='SmartCacheTools',
    version='0.0.0',
    author='Mirco Tracolli',
    author_email='mirco.tracolli@pg.infn.it',
    packages=[
        'Probe',
        'Probe.analyzer',
        'Probe.plotter',
        'Probe.converter',
        'Probe.qTable',
        'DataManager',
        'DataManager.agent',
        'DataManager.agent.converter',
        'DataManager.collector',
        'DataManager.collector.datafeatures',
        'DataManager.collector.datafile',
        'DataManager.collector.dataset',
        'SmartCache',
        'SmartCache.ai',
        'SmartCache.ai.models'
    ],
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
