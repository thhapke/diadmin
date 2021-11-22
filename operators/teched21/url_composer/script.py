# For local development
#from utils.mock_di_api import mock_api
#api = mock_api(__file__,False)

import copy
import io
from datetime import datetime, timedelta
import pandas as pd


def on_input(msg) :

    station_ids = [elem[0] for elem in msg.body]

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
        #att['message.numBatches'] = len(station_ids)
        att['message.lastBatch'] = "False"
        if i == (len(station_ids) -1) :
            att['message.lastBatch'] = "True"
        api.logger.info('GET : {}'.format(att['http.url']))
        out_msg = api.Message(attributes=att,body=None)
        api.send('output',out_msg)    # datatype: message

api.set_port_callback('input',on_input)   # datatype: message

