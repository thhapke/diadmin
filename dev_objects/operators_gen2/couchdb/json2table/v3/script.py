# Mock apis needs to be commented before used within SAP Data Intelligence
#from diadmin.dimockapi.mock_api import api

import json
import pandas as pd

def on_input(msg_id, header, body):

    query_result = json.loads(body.get())

    # Extract the values from the couchdb records
    records = [ v['value'] for v in query_result['rows']]

    # Create a DataFrame (use modified keywords for column names
    df = pd.DataFrame(records)

    # Fill empty values with '' or 0
    for c in df.select_dtypes(include=['float64','int64']):
        df[c].fillna(0,inplace=True)
    for c in df.select_dtypes(include=['object']):
        df[c].fillna('',inplace=True)

    # Ensure received data sequence is the same as required by outport
    outport_vtype_ref = api.DataTypeReference(api.outputs.table.type_ref.type_, api.outputs.table.type_ref.type_id)
    outport_info = api.type_context.get_vtype(outport_vtype_ref)
    outport_col_names = list(outport_info.columns.keys())
    table_list = df[outport_col_names].values.tolist()

    # Send data to outport
    api.outputs.table.publish(api.Table(table_list),header=header)
    

api.set_port_callback('JSON',on_input)