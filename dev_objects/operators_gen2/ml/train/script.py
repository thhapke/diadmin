# Mock apis needs to be commented before used within SAP Data Intelligence
#from diadmin.dimockapi.mock_api import api

import pickle
import io
import pandas as pd

from sklearn.linear_model import LinearRegression

def send_logging(str):
    if api.is_outport_connected["logging"]:
        api.outputs.logging.publish(str,header=None)

df = pd.DataFrame()

def on_input(msg_id, header, data):

    ### GENERIC PART
    global df

    tbl = data.get()
    tbl_info = api.type_context.get_vtype(tbl.type_ref)
    col_names = list(tbl_info.columns.keys())
    if df.empty :
        df = pd.DataFrame(tbl,columns = col_names)
    else :
        df = pd.concat([df,pd.DataFrame(tbl,columns = col_names)])

    send_logging(f"Table: Rows: {df.shape[0]}  Columns:{df.shape[1]}")

    # In case of stream wait for other data
    if 'com.sap.headers.batch' in header and header['com.sap.headers.batch'][1] == False:
        return 1

    send_logging(f"Received batches: df.shape[0]")

    ### TRAINING (Specific)
    model = LinearRegression()
    model.fit(df[['YEAROFREGISTRATION','HP','KILOMETER']].values, df['PRICE'].values)

    ### OUTPUT
    model_binary = pickle.dumps(model)
    bstream = io.BytesIO(model_binary)
    bstream.seek(0)

    api.outputs.model.publish(bstream,-1,header)


api.set_port_callback("input", on_input)

