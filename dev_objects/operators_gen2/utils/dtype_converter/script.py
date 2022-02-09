# For more information about the Python3Operator, drag it to the graph canvas,
# right click on it, and click on "Open Documentation".

# To uncomment the snippets below you can highlight the relevant lines and
# press Ctrl+/ on Windows and Linux or Cmd+/ on Mac.

# # Basic Example 1: Count inputs so far and send on output port (port type
# com.sap.core.string)
# # When using the snippet below make sure you create an output port of type
# #string
# counter = 0
#
# def on_input(msg_id, header, body):
#     global counter
#     counter += 1
#     api.outputs.output.publish(str(counter))
#
# api.set_port_callback("input", on_input)


# # Basic Example 2: Read incoming table as stream and send it
# as stream as well to the output port (any table port type),
# # When using the snippet below make sure you create an input and output
# # port of table type
#
# chunk_size = 10
# 
# # Since this is run in a different thread, exceptions on it will not
# # trigger any action. Alternatives are using `api.propagate_exception
# # or sending through a queue to the operator main thread (the callback one).
# def process_batch(body):
#     try:
#         reader = body.get_reader()
#         # This allows creating one output stream per thread,
#         # each being able to send data in parallel.
#         msg_id, writer = api.outputs.output.with_writer()
#         while True:
#             table = reader.read(chunk_size)
#             # When the stream is closed, len(table) < expected.
#             # If -1 was passed, read would wait for stream to close
#             if len(table) <= 0:
#                 api.logger.info('End of table')
#                 break
#             
#             writer.write(table)
#         writer.close()
#     except Exception as ex:
#         api.propagate_exception(ex)
# 
# def on_input(msg_id, header, body):
#     # Since each input thriggers a thread, it is possible to have
#     # multiple actions happening in parallel.
#     threading.Thread(target=process_batch, args=[body]).start()
# 
# api.set_port_callback("input", on_input)


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
# # Prestart
# # When using the snippet below make sure you create an output port of type
# # int64
# counter = 0
#
# def gen():
#     global counter
#     for i in range(0, 3):
#         api.outputs.output.publish(counter)
#         counter += 1
#
# api.set_prestart(gen)

# # Timer
# # When using the snippet below make sure you create an output port of type
# # binary
# import os
# 
# # Function called when operator handling mode is set to `retry`
# # (more details at the operator documentation)
# def custom_response_callback(msg_id, ex):
#     if ex:
#         api.logger.error("Error when publishing %s: %s" % (str(msg_id), str(ex)))
# 
# 
# def time_callback():
#     dummy_binary = io.BytesIO(os.urandom(20))
#     dummy_header = {"foo": ["bar"]}
#     # Send all binary data at once to the output, if only the first
#     # 10 bytes were to be sent, `n` = 10
#     msg_id = api.outputs.output.publish(dummy_binary, -1,
#     header=dummy_header,
#     response_callback=custom_response_callback)
#     # Controls the time until the next call to time_callback
#     return 1
# 
# api.add_timer(time_callback)

# # Shutdown
# counter = 0
#
# def on_input(msg_id, header, body):
#     global counter
#     counter += 1
#
# api.set_port_callback("input", on_input)
#
# def shutdown1():
#     print("shutdown1: %d" % counter)
#
# api.set_shutdown(shutdown2)
