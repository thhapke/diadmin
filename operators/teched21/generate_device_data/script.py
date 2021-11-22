
import pandas as pd
import numpy as np
import random
import copy
import io
from datetime import timedelta,datetime

#from scipy.spatial import KDTree

CREATE_SQL = False
dtype_map = {'int64':'integer','float64':'float','object':'string','datetime64[ns]':'timestamp'}

def on_input(msg) :


    # config parameter
    #api.config.num_devices = 1000   # datatype : integer
    #api.config.num_device_types = 7   # datatype : integer
    #api.config.prefix_device = 'GRO-'    # datatype : string
    #api.config.start_date = '2014-03-02'    # datatype : string

    stations_df = pd.read_csv(io.BytesIO(msg.body))

    #min max longigtude and latitude of all stations
    min_lat = stations_df['lat'].min()*.99
    max_lat = stations_df['lat'].max()*1.01
    min_long = stations_df['long'].min()*0.99
    max_long = stations_df['long'].max()*1.01

    # generate data SERIALNO, DEVICE_TYPE, INSTALLATION_DATE, LONGITUDE, LATITUDE
    serial_no = np.sort(np.random.choice(api.config.num_devices*1000,api.config.num_devices,replace=False))
    device_types = [ api.config.prefix_device+str(random.randint(100000,1000000)) for n in range(0,api.config.num_device_types)]
    device_types = np.random.choice(device_types,api.config.num_devices,replace=True)

    start_date = datetime.strptime(api.config.start_date,'%Y-%m-%d')
    days = (datetime.now() - start_date).days
    dates = np.random.choice(days,api.config.num_devices,replace=True).tolist()
    dates = [ start_date + timedelta(days=d) for d in dates ]
    lats = np.random.default_rng().uniform(low=min_lat,high=max_lat,size=api.config.num_devices)
    longs = np.random.default_rng().uniform(low=min_long,high=max_long,size=api.config.num_devices)

    #tree = KDTree(np.c_[stations_df['lat'].ravel(),stations_df['long'].ravel()])
    #device_p = np.vstack((lats,longs)).transpose()
    #dd,ii = tree.query(device_p,k=1)
    #print(len(ii))
    #print(dd, ii, sep='\n')

    df = pd.DataFrame({'SERIAL_NO':serial_no,'DEVICE_TYPE':device_types,'INSTALLATION_DATE':dates,
                       'LATITUDE':lats,'LONGITUDE':longs})

    col_types = { col:dtype_map[dt.name] for col,dt in df.dtypes.items() }
    table_dict = {'version':1,'columns':list()}
    for col in df.columns :
        table_dict['columns'].append({'name':col,'class': col_types[col].lower()})

        # ONLY USED for dev
    if CREATE_SQL :
        schema = 'DEVICES'
        table_name = 'DEVICES'
        sql_str = f"CREATE COLUMN TABLE {schema}.{table_name} (\n"
        for col in df.columns :
            data_type = col_types[col]
            sql_str += f'\"{col}\" {data_type}, \n'
        sql_str += "PRIMARY KEY (SERIAL_NO)"
        sql_str += ')'
        print(sql_str)
        return 0

    # Sending to outport output
    att = copy.deepcopy(msg.attributes)
    att['table'] = table_dict
    out_msg = api.Message(attributes=att,body=df.values.tolist())
    api.send('output',out_msg)    # datatype: message

api.set_port_callback('input',on_input)   # datatype: message

