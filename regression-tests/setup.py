from setuptools import setup
from pip.req import parse_requirements


install_reqs = parse_requirements("requirements.txt", session=False)
reqs = [str(ir.req) for ir in install_reqs]

POST="DEV"
BUILD="0"
VERSION="0.7"

setup(
    # Application name:
    name="sparktk-regression-tests",

    version="{0}-{1}{2}".format(VERSION, POST, BUILD),

    # Application author details:
    author="trustedanalytics",


    # Packages
    packages=["automation", "datasets", "exampletests", "generatedata", "sparktkregtests"],

    # Include additional files into the package
    include_package_data=True,

    # Details
    url="https://github.com/trustedanalytics/spark-tk",

    #
    license="Apache 2.0",

    description="spark-tk is a library which enhances the Spark experience by providing a rich, easy-to-use API for Python and Scala.",

    # Dependent packages (distributions)
    install_requires=reqs,
    )
