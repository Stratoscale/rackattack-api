#!/usr/bin/env python
import os
import subprocess
import setuptools
from distutils.core import setup

PKG_INFO = 'PKG-INFO'


def version():
    if os.path.exists(PKG_INFO):
        with open(PKG_INFO) as package_info:
            for key, value in (line.split(':', 1) for line in package_info):
                if key.startswith('Version'):
                    return value.strip()

    return subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip()


setup(name='rackattack-api',
      version=version(),
      description="Rackattack API",
      author='Stratoscale',
      author_email='stratoscale@stratoscale.com',
      keywords='rackattack',
      url='https://github.com/Stratoscale/rackattack-api',
      packages=setuptools.find_packages(exclude=['test']),
      )
