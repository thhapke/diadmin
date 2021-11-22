import sys
from os.path import dirname, join, abspath
import json

proj_dir = join(dirname(dirname(dirname(dirname(abspath(__file__))))))
sys.path.insert(0, proj_dir)

import script
from diadmin.dimockapi.mock_api import mock_api
from diadmin.dimockapi.mock_inport import operator_test

api = mock_api(__file__)     # class instance of mock_api

optest = operator_test(__file__)

connection_file = join('connections/S3Workshops.json')
with open(connection_file) as fp:
    paramsS3 = json.load(fp)

# config parameter
api.config.connectionS3 = {}
api.config.connectionS3["connectionProperties"] = paramsS3[0]['contentData']

api.config.path = None
api.config.role = None
api.config.session = False

msg = api.Message(attributes={'filename':'testfile2.txt'},body='This is a testfile!')

script.on_input(msg)
