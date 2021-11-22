
import pandas as pd
import numpy as np
from random import randint
import copy
from datetime import datetime, timedelta

CREATE_SQL = False
dtype_map = {'int64':'integer','float64':'float','object':'string','datetime64[ns]':'timestamp',
             'datetime64[ns, UTC]':'timestamp'}
             
error_codes = [100,112,231,221,251,122,216,301,304,305,998,999]

def on_input(msg) :

    # Due to input-format PROPOSED transformation into DataFrame
    header = [c['name'] for c in msg.attributes['table']['columns']]
    df = pd.DataFrame(msg.body, columns=header)

    # config parameter 
    #api.config.service_prob = null   # datatype : integer
    base_service_id = int(datetime.now().timestamp())

    df = df.sample(frac=api.config.service_prob,replace=True,axis=0)
    df.drop(columns=['DEVICE_TYPE','LATITUDE','LONGITUDE'],inplace=True)
    df['INSTALLATION_DATE'] = pd.to_datetime(df['INSTALLATION_DATE'])
 
    latest_date = df['INSTALLATION_DATE'].max()

    def random_date(row) :
        rd = randint(0,(latest_date - row['INSTALLATION_DATE']).days)
        return row['INSTALLATION_DATE'] + timedelta(days=rd)

    df['SERVICE_DATE'] = df.apply(random_date,axis = 1 )
    df['SERVICE_ID'] = base_service_id + np.sort(np.random.choice(df.shape[0]*10,df.shape[0],replace=False))
    df['REPAIR_CODE'] = np.random.choice(error_codes,df.shape[0])

    df = df[['SERVICE_ID','SERVICE_DATE','SERIAL_NO','REPAIR_CODE']]
    
    col_types = { col:dtype_map[dt.name] for col,dt in df.dtypes.items() }
    table_dict = {'version':1,'columns':list(),'name':'SERVICES'}
    for col in df.columns :
        table_dict['columns'].append({'name':col,'class': col_types[col].lower()})

        # ONLY USED for dev
    if CREATE_SQL :
        schema = 'DEVICES'
        table_name = 'SERVICES'
        sql_str = f"CREATE COLUMN TABLE {schema}.{table_name} (\n"
        for col in df.columns :
            data_type = col_types[col]
            sql_str += f'\"{col}\" {data_type}, \n'
        sql_str += "PRIMARY KEY (SERVICE_ID)"
        sql_str += ')'
        print(sql_str)

    # Sending to outport output
    att = copy.deepcopy(msg.attributes)
    att['table'] = table_dict
 
    df['SERVICE_DATE'] = df['SERVICE_DATE'].dt.tz_localize(tz=None)
    out_msg = api.Message(attributes=att,body=df.values.tolist())
    
    #return 0 
    api.send('output',out_msg)    # datatype: message

    #df.to_csv('../../../testdata/teched21/generate_device_services/services.csv',index=False)

api.set_port_callback('input',on_input)   # datatype: message.table
