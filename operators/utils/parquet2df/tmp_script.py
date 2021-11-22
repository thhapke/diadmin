
import pandas as pd
import copy
import io


def on_input(msg) :

    # Due to input-format PROPOSED transformation into DataFrame
    df = pd.read_parquet(io.BytesIO(msg.body))

    # config parameter 

    # Sending to outport output
    att = copy.deepcopy(msg.attributes)
    out_msg = api.Message(attributes=att,body=df)
    api.send('output',out_msg)    # datatype: message

api.set_port_callback('input',on_input)   # datatype: message.file

