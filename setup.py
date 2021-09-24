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
    version="0.0.23",
    author="Thorsten Hapke",
    author_email="thorsten.hapke@sap.com",
    description="Utility scripts for SAP Data Intelligence policy management",
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
        'matplotlib'
    ],
    entry_points = {
        'console_scripts': ['dipolicy=diadmin.dipolicy:main',
                            'dipmonitor=diadmin.dipmonitor:main'],
    },
    #package_dir={"": "src"},
    #packages=setuptools.find_packages(),
    packages=['diadmin', 'diadmin.analysis','diadmin.vctl_cmds'],
    python_requires=">=3.6",
)