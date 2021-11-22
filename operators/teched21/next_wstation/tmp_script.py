
import pandas as pd
import numpy as np
import copy
import io
from scipy.spatial import KDTree
from geopy.distance import geodesic

pd.set_option('display.width',500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.max_colwidth',10)

CREATE_SQL = False
dtype_map = {'int64':'integer','float64':'float','object':'string','datetime64[ns]':'timestamp'}


def on_input(msg_stations,msg_devices) :

    stations_df = pd.read_csv(io.BytesIO(msg_stations.body))

    header = [c['name'] for c in msg_devices.attributes['table']['columns']]
    df = pd.DataFrame(msg_devices.body, columns=header)

    # Next neighbour
    tree = KDTree(np.c_[stations_df['LATITUDE'].ravel(),stations_df['LONGITUDE'].ravel()])
    device_p = np.vstack((df['LATITUDE'],df['LONGITUDE'])).transpose()
    _ ,df['NEXT_STATION_ID_INDEX'] = tree.query(device_p,k=1)
    df = pd.merge(df,stations_df,how='inner',left_on='NEXT_STATION_ID_INDEX',right_index=True,suffixes=['_DEVICE','_STATION'])
    df['FROM_DATE'] = pd.to_datetime(df['FROM_DATE'])
    df['TO_DATE'] = pd.to_datetime(df['TO_DATE'])
    latest_date = df['TO_DATE'].max()
    df = df.loc[df['TO_DATE']==latest_date]

    def distance(row) :
        return geodesic((row['LATITUDE_DEVICE'],row['LONGITUDE_DEVICE']),(row['LATITUDE_STATION'],row['LONGITUDE_STATION'])).km
    df['DISTANCE'] = df.apply(distance,axis=1)

    df.drop(columns=['NEXT_STATION_ID_INDEX','TO_DATE'],inplace=True)
    print(df)
    col_types = { col:dtype_map[dt.name] for col,dt in df.dtypes.items() }
    table_dict = {'version':1,'columns':list(),'name':'DEVICE_WEATHERSTATION'}
    table_dict['columns'] = [{'name':col,'class': col_types[col].lower()} for col in df.columns ]

    # ONLY USED for dev
    if CREATE_SQL :
        schema = 'DEVICES'
        table_name = 'DEVICE_WEATHERSTATION'
        sql_str = f"CREATE COLUMN TABLE {schema}.{table_name} (\n"
        for col in df.columns :
            data_type = col_types[col]
            if data_type == 'string' :
                max_len = df[col].str.len().max()
                data_type = f'NVARCHAR({max_len})'
            sql_str += f'\"{col}\" {data_type}, \n'
        sql_str += "PRIMARY KEY (SERIAL_NO)"
        sql_str += ')'
        print(sql_str)
        return 0

    att = copy.deepcopy(msg_devices.attributes)
    att['table'] = table_dict
    out_msg = api.Message(attributes=att,body=df.values.tolist())
    api.send('output',out_msg)    # datatype: message

api.set_port_callback(['stations','devices'],on_input)   # datatype: message

