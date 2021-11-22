# For local development
#from utils.mock_di_api import mock_api
#api = mock_api(__file__,False)


import pandas as pd
from datetime import datetime

import copy


def on_input(blob) :

    # config parameter 
    text = blob.decode("latin-1")
    lines = text.split('\n')

    stations = list()
    for i,line in enumerate(lines[2:]) :
        # skip blank lines
        if len(line.strip()) ==0 :
            continue
        try :
            stations.append({'STATIONS_ID':int(line[:5].strip()),
                             'FROM_DATE':datetime.strptime(line[6:14].strip(),'%Y%m%d'),
                             'TO_DATE':datetime.strptime(line[15:23].strip(),'%Y%m%d'),
                             'HEIGHT':line[25:38].strip(),
                             'LATITUDE':line[39:51].strip(),
                             'LONGITUDE':line[52:61].strip(),
                             'NAME':line[61:101].strip(),
                             'STATE':line[102:].strip()})
        except ValueError as ve :
            api.logger.warning(f"Cannot read line number: {i+2}: {line}")
    api.logger.info(f"Number of stations: {len(stations)}")

    df = pd.DataFrame(stations)
    if api.config.only_running :
        latest_date = df['TO_DATE'].max()
        df = df.loc[df['TO_DATE']==latest_date]

    csv = df.to_csv(index=False,header=True)

    # Sending to outport output
    att = {"date":datetime.today(),"file":{"connection":{"configurationType":"Connection Management","connectionID":"DI_DATA_LAKE"},"path":"/shared/data.csv","size":1},"num":df.shape[0],"operator":"convert_dwdstations"}

    out_msg = api.Message(attributes=att,body=csv)
    api.send('output',out_msg)    # datatype: message

api.set_port_callback('input',on_input)   # datatype: blob
