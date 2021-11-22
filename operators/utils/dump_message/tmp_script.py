
import json
import copy
import pandas as pd


counter = 1
def on_input(msg) :

    global counter

    att = copy.deepcopy(msg.attributes)
    att['file'] = {"connection": {"configurationType": "Connection Management", "connectionID": 'DI_DATA_LAKE' },\
                       "path": '/shared/dump', "size": 1}
    att['msg_name'] = 'msg_'+ str(counter)+'.json'
    if msg.body == None :
        data = 'None'
    elif isinstance(msg.body,pd.DataFrame):
        data = msg.body.to_csv(index = False)
    else :
        data = str(msg.body)

    # dump data
    data_msg = api.Message(attributes=att,body={'attributes':att,'body':data})
    api.send('output',data_msg)    # datatype: message.file

    counter += 1

api.set_port_callback('input',on_input)
