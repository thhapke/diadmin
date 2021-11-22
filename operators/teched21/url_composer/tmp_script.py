

import copy
import io
from datetime import datetime, timedelta
import pandas as pd


def on_input(msg) :

    df = pd.read_csv(io.BytesIO(msg.body))

    #df = pd.DataFrame(msg.body,columns=msg.attributes['header'])

    df['TO_DATE'] = pd.to_datetime(df['TO_DATE'])
    last_date = datetime.now() - timedelta(days=5)
    df = df.loc[df['TO_DATE']>last_date]
    station_ids = df['STATIONS_ID'].unique().tolist()

    for i,station_id in enumerate(station_ids) :

        station_id = str(station_id).zfill(5)
        file = 'tageswerte_KL_' + station_id + '_akt.zip'
        filedata = 'weatherdata_'+ station_id + '.csv'

        # Sending to outport output

        att = dict()
        att['http.method'] = 'GET'
        att['zipfile'] = file
        att['datafilename'] = filedata
        att['http.url'] = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/recent/"+file
        att['message.lastBatch'] = "False"
        if i == (len(station_ids) -1) :
            att['message.lastBatch'] = "True"
        api.logger.info('GET : {}'.format(att['http.url']))
        out_msg = api.Message(attributes=att,body=None)
        api.send('output',out_msg)    # datatype: message

api.set_port_callback('input',on_input)   # datatype: message

