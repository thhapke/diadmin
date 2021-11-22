

def gen() :

    # config parameter 
    #api.config.data = None   # datatype : array

    for i,d in enumerate(api.config.data) :
        lastmsg = True if i == len(api.config.data)-1 else False
        attributes = {'operator':'dispatcher','message.indexBatch':i,'message.lastBatch':lastmsg}
        msg = api.Message(attributes= attributes, body = d)
        api.send('output',msg)

api.add_generator(gen)