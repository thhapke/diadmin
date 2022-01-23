#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
# Using vctl
# URL: https://help.sap.com/viewer/0b99871a1d994d2ea89598fe59d16cf9/3.0.2/en-US/38f6d81551c44f5da0f10bd0249d67f1.html#loio38f6d81551c44f5da0f10bd0249d67f1



import logging
import json
import argparse
import re
import os
from os.path import isdir, join, isfile, basename


def main() :

    logging.basicConfig(level=logging.INFO)

    description =  "Prepare script for offline development"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('operator', help = 'Operator folder')
    parser.add_argument('-w','--overwrite', help = 'Forcefully overwrite existing script',action='store_true')
    args = parser.parse_args()

    # Preliminary checks on arguments
    args.operator = args.operator.replace('.',os.sep)
    operator_dir = join('operators',args.operator)
    if not isdir(operator_dir) :
        raise logging.error('Operator argument are not a folder!')
        return -1

    if not isfile(join(operator_dir,'configSchema.json')) :
        logging.error('Operator folder content invalid. Missing: \'configSchema.json\'!')
        return -1

    if not isfile(join(operator_dir,'operator.json')):
        logging.error('Operator folder content invalid. Missing: \'operator.json\'!')
        return -1

    # Read Operator Json
    with open(join(operator_dir,'operator.json'),'r') as fp:
        opjson = json.load(fp)

    gen2_op = False
    if opjson['component'] == 'com.sap.system.python3Operator.v2':
        logging.info('2nd Generation operator')
        gen2_op = True


    script_file = re.match('file://(.+)',opjson['config']['script']).group(1)
    script_file = join(operator_dir,script_file)
    if isfile(script_file) and not args.overwrite:
        logging.error(f"Script file \'{basename(script_file)}\' exists already. Cannot be modified but only created newly!")
        return -1

    if isfile(script_file) and args.overwrite:
        logging.warning(f'Script-file \'{script_file}\' exists. Will be overwritten!')

    # Read Schema Json
    with open(join(operator_dir,'configSchema.json'),'r') as fp:
        cfgschema = json.load(fp)

    content = '# Mock apis needs to be commented before used within SAP Data Intelligence\n'
    content += 'from diadmin.dimockapi.mock_api import mock_api\n'
    content += 'api = mock_api(__file__)\n\n\n'

    content += 'import copy\n'
    content += 'import pandas as pd\n'
    content += 'import io\n\n\n'

    if "inports" in opjson :
        inport_types = [p['type'] for p in opjson['inports']]
        inport_names = [f"\'{p['name']}\'" for p in opjson['inports']]
    else :
        inport_types = []
        inport_names = []

    len_inports = len(inport_types)
    if len_inports == 0:
        msgs = []
    elif len_inports == 1:
        msgs = ['msg']
    else :
        msgs = [ 'msg_'+ ip['name'] for ip in opjson['inports']]

    if len_inports == 0 :
        call_func = 'gen()'
        content +=f'def {call_func}:\n'
        att = "\tatt = {'generator':'new'}"
    else :
        call_func = f"on_input({','.join(msgs)})"
        content += f"def {call_func}:\n"
        att = f"copy.deepcopy({msgs[0]}.attributes)"

    content += '''
    ### INPUT templates
    # Input message.file to DataFrame
    #df = pd.read_csv(io.BytesIO(msg.body))
    
    # Input message.table to DataFrame
    #header = [c['name'] for c in msg.attributes['table']['columns']]
    #df = pd.DataFrame(msg.body, columns=header)
    
    # Input table to DataFrame
    #df = pd.DataFrame(msg.body, columns=msg.attributes['header'])

    dev_data = None

    ### OUTPUT templates
    # Output DataFrame to message.table
    #dtype_map = {'int64':'integer','float64':'float','object':'string','datetime64[ns]':'timestamp'}
    #col_types = { col:dtype_map[dt.name] for col,dt in df.dtypes.items() }
    #table_dict = {'version':1,'columns':list(),'name':'table'}
    #table_dict['columns'] = [{'name':col,'class': col_types[col].lower()} for col in df.columns ]
    #att['table'] = table_dict
    #dev_data = df.values.tolist()
'''

    for op in opjson['outports']:
        if not gen2_op :
            content +=f'    att = {att}\n'
            content +=f"    msg_{op['name']} = api.Message(attributes=att,body=dev_data)\n"
            content +=f"    api.send('{op['name']}',msg_{op['name']})  # dev_data type: {op['type']}\n\n\n"
        else :
            content +=f"    api.outputs.{op['name']}.publish(data)   #  type: {op['type']}  id: {op['vtype-ID']}\n\n\n"

    if len(inport_types) == 0 :
        if not gen2_op :
            content += "api.add_generator(gen)"
        else:
            content += "    return 1.0\n\n\napi.add_timer(gen)"
    else :
        content += f"api.set_port_callback([{','.join(inport_names)}],on_input)"

    with open(script_file,'w') as fp :
        fp.write(content)

    ## create script_test
    content = r'''import sys
from os.path import dirname, join, abspath
proj_dir = join(dirname(dirname(dirname(dirname(abspath(__file__))))))
sys.path.insert(0, proj_dir)

import script
from diadmin.dimockapi.mock_api import mock_api
from diadmin.dimockapi.mock_inport import operator_test

api = mock_api(__file__)     # class instance of mock_api
mock_api.print_send_msg = True  # set class variable for printing api.send

optest = operator_test(__file__)

# config parameter
'''
    for k,v in cfgschema['properties'].items() :
        if k == 'codelanguage' or k == 'script' :
            continue
        value = "None"
        if k in opjson['config'] :
            value = opjson['config'][k]
        if v['type'] == 'string':
            content += f"api.config.{k} = '{value}' # type: {v['type']}\n"
        else :
            content += f"api.config.{k} = {value} # type: {v['type']}\n"

    content += '\n\n'
    for i,ip in enumerate(inport_types) :
        content += "file = 'testfile.csv'\n"
        content += f"{msgs[i]} = optest.get_msgfile(file)\n\n"

    content += f'script.{call_func}\n'

    script_test_file = script_file[:-3] + '_test.py'
    logging.info(f'Create test script \'{script_test_file}\'')
    with open(script_test_file,'w') as fp :
        fp.write(content)

    ## create test directory
    testdir = join("testdata",args.operator)
    logging.info(f'Make \'testdata\' directory: {testdir}')
    try :
        os.makedirs(testdir)
    except FileExistsError:
        logging.info(f'Directory exists already: {testdir}')


if __name__ == '__main__':
    main()