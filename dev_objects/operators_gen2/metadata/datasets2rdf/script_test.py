import sys
import os
import json
from os.path import dirname, join, abspath

import script
from diadmin.dimockapi.mock_api import api


proj_dir = join(dirname(dirname(dirname(dirname(abspath(__file__))))))
sys.path.insert(0, proj_dir)


api.init(__file__)  # class instance of mock_api


def replace_suffix(filename, new_suffix):
    root_filename, suffix = os.path.splitext(filename)
    if '.' != new_suffix[0]:
        new_suffix = '.' + new_suffix
    return root_filename + new_suffix


# config parameter
#files = ['tmp/datasets_DDL_instances.json', 'tmp/datasets_HANA_DQM.json', 'tmp/datasets_S4HANA.json']
files = ['tmp/datasets_S4HANA.json']
#files = ['tmp/datasets_HANA_DQM.json']

for file in files:

    with open(file) as fp:
        datasets = fp.read()

    # config parameter

    # Start operator script
    msg = api.Message(datasets)
    script.on_input(0, None, msg)

    ttl_bio = api.msg_list[-1]['msg']

    out_file = replace_suffix(file, 'ttl')
    print(f'Write file to: {out_file}')
    with open(out_file, 'wb') as fp:
        fp.write(ttl_bio.getbuffer())

    ttl_bio.seek(0)
    out_file = replace_suffix(file, 'turtle')
    print(f'Write file to: {out_file}')
    with open(out_file, 'wb') as fp:
        fp.write(ttl_bio.getbuffer())
