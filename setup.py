#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#


import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="diadmin",
    version="0.0.64",
    author="Thorsten Hapke",
    license_file = ("Apache-2.0.txt"),
    author_email="thorsten.hapke@sap.com",
    description="Utility scripts for SAP Data Intelligence.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    project_urls={'vctl':"https://launchpad.support.sap.com/#/softwarecenter/template/products/%20_APP=00200682500000001943&_EVENT=DISPHIER&HEADER=Y&FUNCTIONBAR=N&EVENT=TREE&NE=NAVIGATE&ENR=73554900100800002981&V=INST&TA=ACTUAL&PAGE=SEARCH/DATA%20INTELLIGENCE-SYS%20MGMT%20CLI"},
    url="https://github.com/thhapke/diadmin/",
    classifiers=[
         "Programming Language :: Python :: 3.9",
         "License :: OSI Approved :: Apache Software License",
         "Operating System :: OS Independent",
    ],
    include_package_data=True,
    install_requires=[
        'PyYaml',
        'networkx',
        'pandas',
        'matplotlib',
        'rdflib'
    ],
    entry_points = {
        'console_scripts': ['dipolicy=diadmin.dipolicy:main',
                            'dipmonitor=diadmin.dipmonitor:main',
                            'didownload=diadmin.didownload:main',
                            'diupload=diadmin.diupload:main',
                            'diuser=diadmin.diuser:main',
                            'diprettify=diadmin.prettifyJSON:main',
                            's3upload=diadmin.s3upload:main',
                            'dimock=diadmin.dimock:main',
                            'diwsuser=diadmin.diwsuser:main',
                            'diprepreuse=diadmin.prepreuse:main',
                            'dicatalog=diadmin.dicatalog:main',
                            'dibackup=diadmin.dibackup:main',
                            'diconnections=diadmin.diconnections:main',
                            'didockerbuild=diadmin.didockerbuild:main',
                            'didqm=diadmin.didqm:main',
                            'dirun=diadmin.dirun:main',
                            'diopenlog=diadmin.diopenlog:main']
    },
    #package_dir={"": "src"},
    #packages=setuptools.find_packages(),
    packages=['diadmin',
              'diadmin.analysis',
              'diadmin.vctl_cmds',
              'diadmin.dimockapi',
              'diadmin.utils',
              'diadmin.metadata_api',
              'diadmin.pipeline_api'],
    python_requires=">=3.6",
)