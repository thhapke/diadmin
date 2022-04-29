# Mock apis needs to be commented before used within SAP Data Intelligence
#from diadmin.dimockapi.mock_api import api

import pickle
import io

import pandas as pd
from sklearn.linear_model import LinearRegression

#
# GLOBAL VARIABLES
#
df = pd.DataFrame()
all_data_loaded = False
model = None
data_header = None


#
# MODEL INPUT
#
def on_model(msg_id, header, data):
    # GENERIC PART
    global model
    bstream = io.BytesIO(data.get_reader().read(-1))
    model = pickle.load(bstream)
    
    if all_data_loaded:
        process()


#
# DATA INPUT
#
def on_input(msg_id, header, data):

    # GENERIC INPUT
    global df
    global all_data_loaded
    global table_ref
    global data_header

    tbl = data.get()
    tbl_info = api.type_context.get_vtype(tbl.type_ref)
    col_names = list(tbl_info.columns.keys())
    if df.empty:
        df = pd.DataFrame(tbl,columns = col_names)
    else:
        df = pd.concat([df,pd.DataFrame(tbl,columns = col_names)])

    # In case of stream wait for other data
    data_header = header
    if 'com.sap.headers.batch' in header and header['com.sap.headers.batch'][1] == False:
        return 1
    
    all_data_loaded = True
    if model:
        process()


api.set_port_callback("model", on_model)
api.set_port_callback("data", on_input)
    
#
#  Custom Process
#
def process() :
    # PREDICTION
    df['PRICE'] = model.predict(df[['YEAROFREGISTRATION','HP','KILOMETER' ]])
    
    # GENERIC OUTPUT 
    tbl = api.Table(df.values.tolist(),'mycompany.used_cars')
    api.outputs.prediction.publish(tbl,header=data_header)  




