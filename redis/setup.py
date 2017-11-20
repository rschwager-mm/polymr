from setuptools import setup

requires = [
    "polymr",
    "toolz",
    "msgpack-python",
    "redis",
    "hiredis"
]

setup(
    name='polymr_redis',
    version='0.0.1',
    description=("Redis backend for polymr search"),
    py_modules=['polymr_redis'],
    install_requires=requires,
    classifiers=[
        "Development Status :: 2 - Pre-Alpha"
    ],
    test_suite="tests.test_suite"
)
