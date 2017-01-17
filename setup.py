from setuptools import setup

requires = [
    "leveldb",
    "toolz"
]

setup(
    name='polymr',
    version='0.0.1',
    description=("Index and search database tables"),
    packages=['polymr'],
    install_requires=requires,
    classifiers=[
        "Development Status :: 2 - Pre-Alpha"
    ],
    test_suite="tests.test_suite",
    entry_points = { 'console_scripts': [ 'polymr = polymr:cli' ] }
)
