import copy
import json
import io
from diadmin.pipeline_api.runtime import start_batch
from urllib.parse import urljoin

def (msg_id, header, body):

    host = api.config.http_connection['connectionProperties']['host']
    pwd = api.config.http_connection['connectionProperties']['password']
    user = api.config.http_connection['connectionProperties']['user']
    path = api.config.http_connection['connectionProperties']['path']
    tenant = api.config.tenant

    conn = {'url':urljoin(host,path),'auth':(tenant+'\\'+ user,pwd)}

    pipelines = json.load(io.BytesIO(msg.body))
    procs = start_batch(conn, batch=pipelines, max_procs=api.config.multiplicity, sleep_time=api.config.sleep_time)

    #api.send("log",json.dumps(procs))
    
    api.outputs.output.publish(str(counter))


api.set_port_callback("input", on_input)




# # Basic Example 3: State Management support, more details at the operator
# # documentation.
# # When using the snippet below make sure you create input and output ports
# # of the com.sap.core.string type.
# import pickle
# 
# # Internal operator state
# acc = 0
# 
# def on_input(msg_id, header, body):
#     global acc
#     v = int(body.get())
#     acc += v
#     api.outputs.output.publish("%d: %d" % (v, acc))
# 
# api.set_port_callback("input", on_input)
# 
# # It is required to have `is_stateful` set, but since this operator
# # script does not define a generator no information about output port is passed.
# # More details in the operator documentation.
#
# api.set_initial_snapshot_info(api.InitialProcessInfo(is_stateful=True))
# 
# def serialize(epoch):
#     return pickle.dumps(acc)
# 
# api.set_serialize_callback(serialize)
# 
# def restore(epoch, state_bytes):
#     global acc
#     acc = pickle.loads(state_bytes)
# 
# api.set_restore_callback(restore)
# 
# def complete_callback(epoch):
#     api.logger.info(f"epoch {epoch} is completed!!!")
# 
# api.set_epoch_complete_callback(complete_callback)
# 
