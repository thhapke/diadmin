#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
import os
import pandas as pd
import json
import logging


class toapi:
    class Message:
        def __init__(self, body=None, attributes=""):
            self.body = body
            self.attributes = attributes


class operator_test:

    def __init__(self, source_path):
        self.source_path = source_path

    def _filename(self, testdata_file):
        operator_dir = os.path.dirname(self.source_path)
        operator = os.path.basename(operator_dir)
        package_dir = os.path.dirname(operator_dir)
        package = os.path.basename(package_dir)
        project_root = os.path.dirname(os.path.dirname(package_dir))

        return os.path.join(project_root, 'testdata', package, operator, testdata_file)

    def msgtable2df(self, msg):
        header = [c['name'] for c in msg.attributes['table']['columns']]
        return pd.DataFrame(msg.body, columns=header)

    def msgtable2file(self, msg,file):
        df = self.msgtable2df(msg)
        df.to_csv(self._filename(file),index=False)

    def msgfile2file(self, msg,file):
        with open(self._filename(file),'w') as fp:
            fp.write(msg.body)

    #### Return path with test_config
    def get_path(self, config_file):
        actual_path = self._filename(config_file)
        logging.info(actual_path)
        return self._filename(actual_path)

    #### FILE input (simulates File dev_data on inport)
    def get_msgfile(self, testdata_file):
        testfile = self._filename(testdata_file)
        data = open(testfile, mode='rb').read()
        return toapi.Message(attributes={'testfile': testfile}, body=data)

    #### MESSAGE input (dev_data is string)
    def get_msg(self, testdata_file):
        testfile = self._filename(testdata_file)
        with open(os.path.join(testfile), mode='r') as f:
            msg = json.load(f)
        return toapi.Message(attributes=msg['attributes'], body=msg['body'])

    #### TABLE INPUT csv-testdata  (simumlates message.table dev_data on inport)
    def get_msgtable(self, testdata_file):
        classmap = {'int64': 'int', 'float64': 'float64', 'object': 'string', 'bool': 'bool','timestamp':'timestamp'}
        testfile = self._filename(testdata_file)
        fext = os.path.splitext(testfile)[1].lstrip('.')

        if fext == 'csv':
            df = pd.read_csv(testfile)
            columns = []
            name = os.path.basename(testfile).split('.')[0]
            for col in df.columns:
                dty = str(df[col].dtype)
                classtype = classmap[dty]
                columns.append({"class": classtype, "name": col})
            att = {'table': {'columns': columns, 'version': 1,'name':name}, 'table_name':name }
            msg = toapi.Message(attributes=att, body=df.values.tolist())
            return msg
        else:
            raise ValueError('File Extension/Format not supported: {}'.format(fext))

    def get_table(self,testdata_file):
        testfile = self._filename(testdata_file)
        fext = os.path.splitext(testfile)[1].lstrip('.')
        if fext == 'csv':
            df = pd.read_csv(testfile)
            att = {'header':df.columns.tolist(),"message.batchIndex":0,"message.lastBatch":True,
                   "producer.subType":"TABLE","producer.type":"TABLE"}
            msg = toapi.Message(attributes=att, body=df.values.tolist())
            return msg
        else:
            raise ValueError('File Extension/Format not supported: {}'.format(fext))


    # Read file as blob
    def get_blob(self, testdata_file):
        testfile = self._filename(testdata_file)
        with open(testfile, 'rb') as fp:
            blob = fp.read()
        return blob

    def get_json(self,testdata_file):
        testfile = self._filename(testdata_file)
        with open(testfile, 'r') as fp:
            jfile = json.load(fp)
        return jfile

    def get_str(self,testdata_file):
        testfile = self._filename(testdata_file)
        with open(testfile, 'r') as fp:
            str = fp.read()
        return str

    def save_bytefile(self,filename,content):
        savefile = self._filename(filename)
        with open(savefile, 'wb') as fp:
            fp.write(content)

    def save_csvfile(self,filename,content):
        savefile = self._filename(filename)
        df = self.msgtable2df(content)
        df.to_csv(savefile,index = False)

    def save_strfile(self,filename,content):
        savefile = self._filename(filename)
        with open(savefile, 'w') as fp:
            fp.write(content)