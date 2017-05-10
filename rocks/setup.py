from setuptools import setup

requires = [
    "polymr",
    "toolz",
    "msgpack-python",
    "python-rocksdb"
]

setup(
    name='polymr_rocksdb',
    version='0.0.1',
    description=("Rocksdb backend for polymr search"),
    packages=['polymr_rocksdb'],
    install_requires=requires,
    classifiers=[
        "Development Status :: 2 - Pre-Alpha"
    ],
    test_suite="tests.test_suite"
)
