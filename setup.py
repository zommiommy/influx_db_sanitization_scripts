import os

from setuptools import find_packages, setup

desc = "Collection of scripts to clean a influx db",

test_deps =[
    "pytest",
    "pytest-cov"
]

extras = {
    'test': test_deps,
}

setup(
    name='influxdb_sanitization_scripts',
    version="1.0.0",
    description=desc,
    long_description=desc,
    url="https://github.com/zommiommy/influx_db_sanitization_scripts",
    author="Tommaso Fontana",
    author_email="tommaso.fontana.96@gmail.com",
    # Choose your license
    license='MIT',
    include_package_data=True,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: GPL2 License',
        'Programming Language :: Python :: 3'
    ],
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    tests_require=test_deps,
    # Add here the package dependencies
    install_requires=[
        "numpy",
        "pandas",
        "influxdb",
        "humanize",
        "tqdm"
    ],
    extras_require=extras,
)