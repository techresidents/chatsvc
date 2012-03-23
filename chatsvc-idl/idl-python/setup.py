#!/usr/bin/env python

from distutils.core import setup

setup(name='techresidents services chatsvc',
      version='${project.version}',
      description='30and30 Service',
      packages=['techresidents',
                'techresidents.services',
                'techresidents.services.chatsvc',
                'techresidents.services.chatsvc.gen']
    )

