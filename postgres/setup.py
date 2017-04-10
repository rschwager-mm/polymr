from setuptools import setup

requires = [
    "polymr",
    "toolz",
    "msgpack-python",
    "py-postgresql<=1.2.1"
]

setup(
    name='polymr_postgres',
    version='0.0.1',
    description=("Postgres backend for polymr search"),
    packages=['polymr_postgres'],
    install_requires=requires,
    classifiers=[
        "Development Status :: 2 - Pre-Alpha"
    ],
    test_suite="tests.test_suite"
)
