from setuptools import setup

requires = [
    "polymr",
    "toolz",
    "msgpack-python",
    "boto"
]

setup(
    name='polymr_dynamodb',
    version='0.0.1',
    description=("Dynamodb backend for polymr search"),
    py_modules=['polymr_dynamodb'],
    install_requires=requires,
    classifiers=[
        "Development Status :: 2 - Pre-Alpha"
    ],
    test_suite="tests.test_suite"
)
