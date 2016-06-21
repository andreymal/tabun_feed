#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


setup(
    name='tabun_feed',
    version='0.6.1',
    description='Watcher of new content on tabun.everypony.ru',
    author='andreymal',
    author_email='andriyano-31@mail.ru',
    license='MIT',
    url='https://github.com/andreymal/tabun_feed',
    platforms=['linux', 'osx', 'bsd'],
    packages=find_packages(),
    include_package_data=True,
    install_requires=['tabun_api>=0.7.0'],
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'tabun_feed=tabun_feed.runner:main',
            'tf_manage=tabun_feed.manage:main',
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
)
