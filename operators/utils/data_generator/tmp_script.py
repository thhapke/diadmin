
import pandas as pd
import os
import time

counter = 0
def gen() :

    global counter
    counter +=1
    # config parameter 
    #api.config.period = null   # datatype : integer

    api.send('output','{},{},test'.format(api.config.pipeline,counter))
    time.sleep(api.config.period)


api.add_generator(gen)