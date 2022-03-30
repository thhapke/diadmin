# Mock apis needs to be commented before used within SAP Data Intelligence
#from diadmin.dimockapi.mock_api import mock_api
#api = mock_api(__file__)

from urllib.parse import urljoin
import pandas as pd

from diadmin.metadata_api import container

#def on_input(msg):
def gen() :
    
    url = api.config.http_connection['connectionProperties']['host']
    user = api.config.http_connection['connectionProperties']['user']
    pwd = api.config.http_connection['connectionProperties']['password']
    path = api.config.http_connection['connectionProperties']['path']
    conn = {'url': urljoin(url, path),'auth': (user, pwd)}
    
    header_values = [conn['url'],'user','pwd']
    header = {"diadmin.metadata.header":header_values}
    
    df = container.export_catalog(conn)
    df['new_dataset_tags'] = ''
    df['new_column_tags'] = ''
    df = df[["connection_type","connection_id","dataset_path","dataset_name","column_name","length","dtype",
             "dataset_tags","column_tags",
             "new_dataset_tags","new_column_tags"]]

    tbl = api.Table(df.values.tolist(),"diadmin.metadata")
    
    api.outputs.data.publish(tbl,header)   
    return 1000000
    
    
    
api.add_timer(gen)  
#api.set_port_callback(['trigger'],on_input)
