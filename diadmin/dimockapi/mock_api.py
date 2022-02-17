#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
import os
import json
import logging
from typing import List


class outport :
    def __init__(self,port_name,msg_list):
        self.port_name = port_name
        self.msgs = msg_list
    def publish(self,data,header=None):
        self.msgs.append({'port':self.port_name,'dev_data':data})


class mock_logger :

    def info(self,msg_str):
        logging.info(msg_str)
    def debug(self,msg_str):
        logging.debug(msg_str)
    def warning(self,msg_str):
        logging.warning(msg_str)
    def error(self,msg_str):
        logging.error(msg_str)
    def addHandler(self,handler):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        logger = logging.getLogger(name="operator")
        logger.addHandler(handler)


class mock_table:
    def __init__(self, body: List[list] = [], type_id: str = u""):

        self.type_id = type_id
        self.base_type_kind = "table"
        if not isinstance(body, list):
            raise Exception(
                f"Body must of type list, not of {type(body)}")
        self.body = body
        self.index = 0

        if type_id != u"":
            self.type_ref = "mock_type"
        else:
            self.type_ref = None



class api:

    print_send_msg = True
    msg_list = list()
    logger = mock_logger()
    stateful_flag = True
    epoch = None
    Table = mock_table

    class config :
        op_dir = ""

    class outputs :
        pass

    def add_config(source_path):
        api.config.op_dir = os.path.dirname(source_path)
        with open(os.path.join(api.config.op_dir,'operator.json')) as json_file:
            config_data = json.load(json_file)
        for k, v in config_data['config'].items():
            if k in ['errorHandling','$type'] :
                continue
            setattr(api.config, k, v)

    def add_ports(source_path) :
        op_dir = os.path.dirname(source_path)
        with open(os.path.join(op_dir,'operator.json')) as json_file:
            config_data = json.load(json_file)
        for op in config_data['outports']:
            setattr(api.outputs,op['name'],outport(op['name'],api.msg_list))

    def init(source_path):
        api.add_config(source_path)
        api.add_ports(source_path)

    class Message:
        def __init__(self, body=None, attributes=""):
            self.body = body
            self.attributes = attributes

    def send(port,msg):
        api.msg_list.append({'port':port, 'dev_data':msg})
        if api.print_send_msg :
            if isinstance(msg,str) :
                print('PORT {}: {}'.format(port,msg))
            else :
                print('PORT {}: \nattributes: {}\nbody: {}'.format(port,str(msg.attributes),str(msg.body)))

    def set_port_callback(*args):
        pass
    def add_generator(*args):
        pass
    def add_timer(timer_func):
        api.call_timer = timer_func
    def add_shutdown_handler(*args):
        pass

    # Gen 2
    def set_initial_snapshot_info(*args) :
        pass
    def set_epoch_complete_callback(*args):
        pass
    def InitialProcessInfo(is_stateful) :
        api.stateful_flag = is_stateful
    def complete_callback(epoch):
        api.epoch = epoch
    def set_restore_callback(restore_func) :
        api.restore = restore_func
    def set_serialize_callback(serialize_func) :
        api.serialize = serialize_func


