from setuptools_plugins.version import get_version
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="rackattack_api",
    packages=setuptools.find_packages(where="py"),
    package_dir={"": "py"},
    setup_requires=['python-setuptools-plugins'],
    version=get_version(),
    author="Stratoscale",
    author_email="zcompute@zadarastorage.com",
    description="API for provisioning rackattack hosts",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Stratoscale/rackattack-api",
    project_urls={
        "Bug Tracker": "https://github.com/Stratoscale/rackattack-api/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 2",
        "License :: Apache License 2.0",
        "Operating System :: OS Independent",
    ],
    options={'bdist_wheel': {'universal': True}}
)
