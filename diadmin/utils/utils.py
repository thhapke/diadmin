#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
import re
import logging
import csv
import json
from os import path, listdir,mkdir, rename

def add_defaultsuffix(file, suffix) :
    suffix = suffix.strip('.')
    if not re.match(f'.+\.{suffix}',file) :
        file += '.'+suffix
    return file


def toggle_mockapi_file(file,comment=False) :
    if not comment :
        logging.info(f"Uncomment 'mockapi' from script: {file}")
    else :
        logging.info(f"Comment 'mockapi' from script: {file}")
    script = ''
    with open(file,'r') as fp:
        line = fp.readline()
        while(line) :
            if comment :
                # from utils.mock_di_api import mock_api
                if re.match('from\s+utils.mock_di_api\s+import\s+mock_api',line) :
                    line = re.sub('from\s+utils.mock_di_api\s+import\s+mock_api','#from utils.mock_di_api import mock_api',line)
                # api = mock_api
                if re.match('api\s*=\s*mock_api',line) : #api = mock_api(__file__)
                    line = re.sub('api\s*=\s*mock_api','#api = mock_api',line)
                # api.init(__file__)
                if re.match('api.init\(__file__\)',line) : #api.init(__file__)
                    line = re.sub('api.init\(__file__\)','#api.init(__file__)',line)
                # from diadmin.dimockapi.mock_api import mock_api
                if re.match('from\s+diadmin.dimockapi.mock_api\s+import\s+api',line) :
                    line = re.sub('from\s+diadmin.dimockapi.mock_api\s+import\s+api','#from diadmin.dimockapi.mock_api import api',line)
            else:
                if re.match('#from\s+utils.mock_di_api\s+import\s+mock_api',line) :
                    line = re.sub('#from\s+utils.mock_di_api\s+import\s+mock_api','from utils.mock_di_api import mock_api',line)
                if re.match('#api\s*=\s*mock_api',line) : #api = mock_api(__file__)
                    line = re.sub('#api\s*=\s*mock_api','api = mock_api',line)
                if re.match('#api.init\(__file__\)',line) : #api = mock_api(__file__)
                    line = re.sub('#api.init\(__file__\)','api.init(__file__)',line)
                # from diadmin.dimockapi.mock_api import mock_api
                if re.match('#from\s+diadmin.dimockapi.mock_api\s+import\s+mock_api',line) :
                    line = re.sub('#from\s+diadmin.dimockapi.mock_api\s+import\s+api','from diadmin.dimockapi.mock_api import api',line)

            script += line
            line = fp.readline()
    with open(file,'w') as fp :
        fp.write(script)

def toggle_mockapi(dir,comment) :
    logging.info(f'Folder: {dir}')
    if path.isfile(path.join(dir,'operator.json')) :
        script_name = get_script_name(dir)
        if script_name :
            toggle_mockapi_file(path.join(dir,script_name),comment)
    for sd in listdir(dir) :
        if path.isdir(path.join(dir,sd)) :
            toggle_mockapi(path.join(dir,sd),comment)

def get_script_name(dir) :
    with open(path.join(dir,'operator.json')) as jf :
        opjson = json.load(jf)
    script_name = opjson['config']['script'][7:]
    if not re.match('file://',script_name) :
        logging.info(f"No separate script,is embedded in \'operator.json\'")
        return None
    logging.info(f"Script: {script_name}")
    return script_name

def get_operator_generation(dir) :
    with open(path.join(dir,'operator.json')) as jf :
        opjson = json.load(jf)
    gen = 1
    if opjson['component'] == "com.sap.system.python3Operator.v2":
        gen = 2
    return gen


########## Get di system id
def get_system_id(cluster,tenant):
    disystem = re.match('.+vsystem\.ingress\.([a-zA-Z0-9-_]+)\.([a-zA-Z0-9-_]+).+',cluster)
    return disystem.group(1) + '.' + disystem.group(2) + '_'+ tenant

######### Files
class csvlist :
    def __init__(self,filename,keys=None):
        self.filename = filename
        self.index = -1
        self.records = list()
        self.keys = keys
        self.with_comments = False
        self.filter = None
        self.no_header = True
        self.placeholder = dict()
        with open(filename,mode='r',newline='\n') as csvfile :
            csvreader = csv.reader(csvfile,delimiter = ',')
            if keys == None:
                self.keys = next(csvreader)
                self.placeholder = { k:'' for k in self.keys}
                self.no_header = False
            for line in csvreader:
                if line[0][0] == '#' :
                    self.records.append(','.join(line))
                else :
                    self.records.append(dict(zip(self.keys,line)))

    def __iter__(self):
        return self

    def __next__(self):
        self.index += 1
        if self.index >= len(self.records):
            raise StopIteration
        while  (self.with_comments == False and isinstance(self.records[self.index],str)) or \
                (not self.filter == None and not self.records[self.index][self.filter[0]] == self.filter[1]):
            self.index += 1
            if self.index >= len(self.records):
                raise StopIteration
        return self.records[self.index]

    def save(self):
        with open(self.filename,mode='w',newline='\n') as csvfile :
            if self.no_header == False :
                csvfile.write(','.join(self.keys)+'\n')
            for r in self.records:
                if isinstance(r,str) :
                    csvfile.write(r+'\n')
                else :
                    csvfile.write(','.join(r.values())+'\n')

    def extend(self,records):
        # add missing values with default values
        for new_rec in records:
            missing_values = { k:'' for k in (set(self.keys).difference(new_rec.keys()))}
            new_rec.update(missing_values)
            found = False
            for rec in self.records:
                if isinstance(rec,str) :
                    continue
                shared_values = [k for k in rec if rec[k] == new_rec[k] or new_rec[k]=='']
                if  len(shared_values) == len(self.keys) :
                    found = True
                    break
            if not found:
                self.records.append(new_rec)
        self.save()

    def remove(self,record):
        logging.info(f"Remove record: {record}")
        self.records.remove(record)
        self.save()

    def set_default(self,defaults):
        for rec in self.records:
            if isinstance(rec,str) :
                continue
            for k,v in defaults.items() :
                if rec[k] == '':
                    rec[k] = v
        self.save()


def mksubdir(parentdir,dir) :
    newdir = path.join(parentdir,dir)
    if path.isfile(newdir) :
        return None
    elif not path.isdir(newdir) :
        logging.info(f"Make directory: {newdir}")
        return mkdir(newdir)
    else :
        return newdir

def mksubdirs(*args) :
    dir_path =args[0]
    for d in args[1:] :
        dir_path = path.join(dir_path,d)
    dirs_list = dir_path.split(path.sep)
    if re.match(r'.+\.\w{3}$',dirs_list[-1]):
        dirs_list = dirs_list[:-1]
    adir = dirs_list[0]
    for i in range(1,len(dirs_list)) :
        mksubdir(adir,dirs_list[i])
        adir = path.join(adir,dirs_list[i])

