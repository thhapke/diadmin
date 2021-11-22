
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

    # convert to json
    data = datastr.split('\n')
    records = list()
    recs = [ [rd.strip() for rd in d.split(';') ] for d in data]

    map_col = {'STATIONS_ID': 'STATIONS_ID', 'MESS_DATUM': 'MEASUREMENT_DATE', 'QN_3': 'QUALITY_LEVEL_3', 'FX': 'MAX_WINDGUST',
               'FM': 'MEAN_WIND_VELOCITY', 'QN4': 'QUALITY_LEVEL_4', 'RSK': 'PRECIPITATION_HEIGHT',
               'RSKF': 'PRECIPITATION_TYPE', 'SDK': 'SUN_DURATION', 'SHK_TAG': 'SNOW_DEPTH', 'NM': 'CLOUD_COVER',
               'VPM': 'VAPOR_PRESSURE', 'PM': 'MEAN_PRESSURE', 'TMK': 'MEAN_TEMPERATURE', 'UPM': 'MEAN_REL_HUMIDITY',
               'TXK': 'MAX_TEMPERATURE', 'TNK': 'MIN_TEMPERATURE', 'TGK': 'MIN_AIR_TEMPERATURE', 'eor': 'EOR'}

    col_type = {'STATIONS_ID': 'INTEGER', 'MEASUREMENT_DATE': 'TIMESTAMP', 'MAX_WINDGUST':'FLOAT',
                 'MEAN_WIND_VELOCITY':'FLOAT','PRECIPITATION_HEIGHT':'FLOAT',
                 'PRECIPITATION_TYPE':'INTEGER','SUN_DURATION':'FLOAT','SNOW_DEPTH':'FLOAT','CLOUD_COVER':'FLOAT',
                 'VAPOR_PRESSURE':'FLOAT', 'MEAN_PRESSURE':'FLOAT','MEAN_TEMPERATURE':'FLOAT', 'MEAN_REL_HUMIDITY':'FLOAT',
                 'MAX_TEMPERATURE':'FLOAT', 'MIN_TEMPERATURE':'FLOAT','MIN_AIR_TEMPERATURE':'FLOAT'}

    df = pd.DataFrame(recs[1:],columns=map_col.values())
    df.drop(columns=['EOR','QUALITY_LEVEL_3','QUALITY_LEVEL_4'],inplace=True)
    df.dropna(axis=0,thresh=3,inplace=True)
    df['MEASUREMENT_DATE'] = df['MEASUREMENT_DATE'].apply(lambda x: datetime.strptime(x,'%Y%m%d').isoformat())

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

    table_dict = {'version':1,'columns':list()}
    for col in df.columns :
        table_dict['columns'].append({'name':col,'class': col_type[col].lower()})

    # Sending to outport output
    att = copy.deepcopy(msg.attributes)
    att['table'] = table_dict
    #out_msg = api.Message(attributes=att,body=df.to_csv(index=False))
    out_msg = api.Message(attributes=att,body=df.values)
    api.send('output',out_msg)    # datatype: message

api.set_port_callback('input',on_input)   # datatype: message

