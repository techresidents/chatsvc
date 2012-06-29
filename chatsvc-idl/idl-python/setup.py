#!/usr/bin/env python

from distutils.core import setup

setup(name='trchatsvc',
      version='${project.version}',
      description='Tech Residents Service',
      packages=['trchatsvc',
                'trchatsvc.gen']
    )

