# Mock apis needs to be commented before used within SAP Data Intelligence
#from diadmin.dimockapi.mock_api import api
#api.init(__file__)

from datetime import datetime, timezone
import string
import pickle
import time

import pandas as pd

from operators.utils.big_data_generator.data_generator import data_generator


dg = data_generator(5,5)

def gen():

    api.logger.info(f'Create new DataFrame: {dg.batch}')
    dg.next()
    
    # Create Header
    header_values = [dg.batch,api.config.max_index,api.config.num_rows,float(api.config.periodicity)]
    header = {"diadmin.headers.utils.performance_test":header_values}
    header_dict = dict(zip(['index','max_index','num_rows','periodicity'],header_values))    
    api.logger.debug(f'Header: {header_dict}')
    
    # Check if graph should terminate
    if dg.batch >= int(api.config.max_index) or int(api.config.max_time) < dg.running_time() : 
        api.logger.info(f'Send msg to port \'stop\' Batch: {dg.batch}/{api.config.max_index}')
        api.logger.info(f'Send msg to port \'stop\' Running Minutes: {dg.running_time()}/{api.config.max_time}')
        api.outputs.stop.publish(True,header=header)
        return 0
    
    # forced exception
    if dg.batch == api.config.crash_index and (not dg.crashed_already) :
        dg.crashed_already = True
        time.sleep(api.config.snapshot_time)
        raise ValueError(f"Forced Crash: {dg.crashed_already}")

    # convert df to table including data type cast
    dg.data['timestamp'] = dg.data['timestamp'].apply(pd.Timestamp.isoformat)
    dg.data = dg.data[['batch','id','timestamp','string_value','float_value']]
    tbl = api.Table(dg.data.values.tolist(),"diadmin.utils.performance_test")
    
    # output port
    api.outputs.output.publish(tbl,header=header)  
    
    # log port
    api.outputs.log.publish(f"{dg.batch} - {dg.data.head(3)}")   

    return int(api.config.periodicity)
    

def serialize(epoch):
    api.logger.info(f"Serialize {epoch}: Batch: {dg.batch} - crashed_already:{dg.crashed_already}")
    return pickle.dumps(dg)

def restore(epoch, state_bytes):
    global dg
    dg = pickle.loads(state_bytes)
    api.logger.info(f"Restore {epoch}: Batch: {dg.batch} - crashed_already:{dg.crashed_already}")

def complete_callback(epoch):
    api.outputs.log.publish(f"epoch {epoch} is completed!!!")


api.add_timer(gen)    
api.set_serialize_callback(serialize)
api.set_restore_callback(restore)
api.set_epoch_complete_callback(complete_callback)
api.set_initial_snapshot_info(api.InitialProcessInfo(is_stateful=True))