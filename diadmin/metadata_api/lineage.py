#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
########################################################
#### Using SAP Data Intelligence API Business Hub
#### Doc: https://api.sap.com/api/metadata/resource
########################################################
import logging
import sys
from urllib.parse import urljoin
import urllib
from os import path
import re
import csv


import requests
import json
import yaml
import pandas as pd


from diadmin.utils.utils import get_system_id
from diadmin.metadata_api.catalog import download_hierarchies
from diadmin.connect.connect_neo4j import neo4jConnection


#########
# MAIN
########
if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)

    with open('config_demo.yaml') as yamls:
        params = yaml.safe_load(yamls)

    connection = {'url': urljoin(params['URL'], '/app/datahub-app-metadata/api/v1'),
                  'auth': (params['TENANT'] + '\\' + params['USER'], params['PWD'])}

    if 'GRAPHDB' in params:
        connection['GRAPHDB'] = params['GRAPHDB']
        connection['TENANT'] = params['TENANT']
        connection['URL'] = params['URL']


    sysid = get_system_id(params['URL'],params['TENANT'])

