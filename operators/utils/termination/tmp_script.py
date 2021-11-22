
def on_input(msg) :

    if ('message.lastBatch' in msg.attributes and
            (msg.attributes['message.lastBatch'] == True or
             msg.attributes['message.lastBatch'] == 1 or
             msg.attributes['message.lastBatch'] == "True")):
        api.send('output',msg)    # datatype: message

api.set_port_callback('input',on_input)   # datatype: message.*

