# Mock apis needs to be commented before used within SAP Data Intelligence
from diadmin.dimockapi.mock_api import api

import json
import pandas as pd

def on_input(msg_id, header, body):

    query_result = json.loads(body.get())

    # Extract the values from the couchdb records
    records = [ v['value'] for v in query_result['rows']]

    # Create a DataFrame (use modified keywords for column names
    df = pd.DataFrame(records)
    col_map = { c : c[6:].upper() for c in df.columns}  # WARNING Data Specific
    df.rename(columns=col_map,inplace=True)

    # Fill empty values with '' or 0
    for c in df.select_dtypes(include=['float64','int64']):
        df[c].fillna(0,inplace=True)
    for c in df.select_dtypes(include=['object']):
        df[c].fillna('',inplace=True)

    # Ensure received data sequence is the same as required from outport (Required update: outport vtype retrieval)
    output_info = api.type_context.get_vtype(api.DataTypeReference("table", "diadmin.mc.visits"))
    output_col_names = list(output_info.columns.keys())
    table_list = df[output_col_names].values.tolist()

    # OPTION: Write into stream
    #batch_size = 1000
    #msg_id, writer = api.outputs.table.with_writer()
    #for row_index in range(0,len(table_list),batch_size):
    #     api.logger.info(f"Send batch {row_index} - {row_index+batch_size} ({len(table_list)})")
    #     batchtable = api.Table(table_list[row_index:row_index+batch_size])
    #     api.outputs.table.publish(batchtable,header=header)
    #     #writer.write(batchtable)
    #writer.close()

    # Publish data
    batchtable = api.Table(table_list[0:5])
    api.outputs.table.publish(batchtable,header=header)
    #api.outputs.table.publish(table,header=header)


api.set_port_callback('JSON',on_input)