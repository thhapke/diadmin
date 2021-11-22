# For local development
#from utils.mock_di_api import mock_api
#api = mock_api(__file__,False)


import pandas as pd
import copy
import zipfile
import re
import io
from datetime import datetime

CREATE_SQL = False


def on_input(msg) :

    api.logger.info('Unzip: {}'.format(msg.attributes['zipfile']))

    try :
        with zipfile.ZipFile(io.BytesIO(msg.body)) as zipp :
            dwd_files = zipp.namelist()
            r = re.compile("produkt_klima_tag.+\.txt")
            datafile = list(filter(r.match, dwd_files))[0]
            datastr = zipp.read(datafile).decode('latin-1')
    except zipfile.BadZipFile  as bzf:
        print(str(bzf))
        api.logger.warning('Bad zip file: \"{}\" ({})'.format(msg.attributes['zipfile'],str(bzf)))
        return -1

    data = datastr.split('\n')
    recs = [ [rd.strip() for rd in d.split(';') ] for d in data]

    map_col = {'STATIONS_ID': 'STATIONS_ID', 'MESS_DATUM': 'MEASUREMENT_DATE', 'QN_3': 'QUALITY_LEVEL_3', 'FX': 'MAX_WINDGUST',
               'FM': 'MEAN_WIND_VELOCITY', 'QN4': 'QUALITY_LEVEL_4', 'RSK': 'PRECIPITATION_HEIGHT',
               'RSKF': 'PRECIPITATION_TYPE', 'SDK': 'SUN_DURATION', 'SHK_TAG': 'SNOW_DEPTH', 'NM': 'CLOUD_COVER',
               'VPM': 'VAPOR_PRESSURE', 'PM': 'MEAN_PRESSURE', 'TMK': 'MEAN_TEMPERATURE', 'UPM': 'MEAN_REL_HUMIDITY',
               'TXK': 'MAX_TEMPERATURE', 'TNK': 'MIN_TEMPERATURE', 'TGK': 'MIN_AIR_TEMPERATURE', 'eor': 'EOR'}

    col_type = {'STATIONS_ID': 'integer', 'MEASUREMENT_DATE': 'timestamp', 'MAX_WINDGUST':'float',
                 'MEAN_WIND_VELOCITY':'float','PRECIPITATION_HEIGHT':'float',
                 'PRECIPITATION_TYPE':'integer','SUN_DURATION':'float','SNOW_DEPTH':'float','CLOUD_COVER':'float',
                 'VAPOR_PRESSURE':'float', 'MEAN_PRESSURE':'float','MEAN_TEMPERATURE':'float', 'MEAN_REL_HUMIDITY':'float',
                 'MAX_TEMPERATURE':'float', 'MIN_TEMPERATURE':'float','MIN_AIR_TEMPERATURE':'float'}

    df = pd.DataFrame(recs[1:],columns=map_col.values())
    df.drop(columns=['EOR','QUALITY_LEVEL_3','QUALITY_LEVEL_4'],inplace=True)
    for col in df.columns :
        if col == 'MEASUREMENT_DATE' :
            df['MEASUREMENT_DATE'] = pd.to_datetime(df['MEASUREMENT_DATE'],format='%Y%m%d')
        else :
            df[col] = pd.to_numeric(df[col])
    df.replace(-999,pd.NA,inplace=True)
    df.dropna(axis=0,thresh=3,inplace=True)
    df.fillna(-999,inplace=True)
    #df.dropna(axis=1,how='any',inplace=True)


    #df.replace('-999',pd.NA,inplace=True)

    #df['MEASUREMENT_DATE'] = df['MEASUREMENT_DATE'].apply(lambda x: datetime.strptime(x,'%Y%m%d').strftime('%Y-%m-%d %H:%M:%S.%f0'))

    # ONLY USED for dev
    if CREATE_SQL :
        schema = 'DEVICES'
        table_name = 'DAILY_WEATHER'
        sql_str = f"CREATE COLUMN TABLE {schema}.{table_name} (\n"
        for col in df.columns :
            data_type = col_type[col]
            sql_str += f'\"{col}\" {data_type}, \n'
        sql_str += "PRIMARY KEY (STATIONS_ID,MEASUREMENT_DATE)"
        sql_str += ')'
        print(sql_str)
        return 0

    table_dict = {'version':1,'name':'DAILY_WEATHER','columns':list()}
    for col in df.columns :
        table_dict['columns'].append({'name':col,'class': col_type[col].lower()})

    # Sending to outport output
    att = copy.deepcopy(msg.attributes)
    att['table'] = table_dict
    df.fillna('', inplace=True)
    table_data = df.values.tolist()
    out_msg = api.Message(attributes=att,body=table_data)
    api.logger.info(f'Send to DB ROW 0 : {table_data[0]}')
    api.send('output',out_msg)    # datatype: message

api.set_port_callback('input',on_input)   # datatype: message

