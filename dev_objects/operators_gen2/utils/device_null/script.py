import pandas as pd


def on_input(msg_id, header, data):
    
    header_dict = dict(zip(['index','max_index','num_rows','periodicity'],list(header.values())[0]))
    
    if api.config.batch_size == -1 :
        tbl = data.get()
        api.outputs.log.publish(f"Batch: {header_dict['index']} Num Records: {len(tbl)}",header=header) 
        tbl_info = api.type_context.get_vtype(tbl.type_ref)
        col_names = list(tbl_info.columns.keys())
        df = pd.DataFrame(tbl.body,columns = col_names)
    else :
        table_reader = data.get_reader()
        tbl = table_reader.read(2)
        tbl_info = api.type_context.get_vtype(tbl.type_ref)
        col_names = list(tbl_info.columns.keys())
        df = pd.DataFrame(tbl.body,columns = col_names)
        while len(tbl) > 0 :
            api.outputs.log.publish(f"Stream: {header_dict['index']} Num Records: {len(tbl)}",header=header) 
            tbl = table_reader.read(2)
            dft = pd.DataFrame(tbl.body,columns = col_names)
            df.append(dft, ignore_index=True)
        

api.set_port_callback("input", on_input)