#!/usr/bin/env python

from distutils.core import setup

setup(name='trchatsvc',
      version='${project.version}',
      description='30and30 Service',
      packages=['trchatsvc',
                'trchatsvc.gen']
    )

