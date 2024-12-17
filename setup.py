# setup.py

from setuptools import setup, find_packages

setup(
    name="mathlib",            # Name of your library
    version="0.1.0",           # Version
    description="A simple math library for addition and subtraction.",
    author="Your Name",
    packages=find_packages(),  # Automatically find all packages
    install_requires=[
         "simplejson"  # Dependency added
    ],       # List dependencies here
)


print("building..")
