
import pandas as pd
import copy
import io


def on_input(msg) :

    # Due to input-format PROPOSED transformation into DataFrame
    bstream = io.BytesIO(msg.body)
    element = bstream.readline().strip().decode('utf-8')
    data = list()
    while element:
        data.append(element)
        element = bstream.readline().strip().decode('utf-8')

    # config parameter

    # Sending to outport output
    att = copy.deepcopy(msg.attributes)
    out_msg = api.Message(attributes=att,body=data)
    api.send('output',out_msg)    # datatype: message

api.set_port_callback('input',on_input)   # datatype: message.file

