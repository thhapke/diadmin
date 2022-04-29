import sys
from os.path import dirname, join, abspath
proj_dir = join(dirname(dirname(dirname(dirname(abspath(__file__))))))
sys.path.insert(0, proj_dir)

import logging
import pickle
import pandas as pd
import numpy as np

import script
from diadmin.dimockapi.mock_api import api

logging.basicConfig(level=logging.INFO)
api.init(__file__)     # class instance of mock_api

df = pd.read_csv("/Users/Shared/data/ml/USEDCARS_40K.csv")
header = {'com.sap.headers.batch':[0,False,0,0,'unit']}

index = 0
isLast = False
num_rows = 0
num_batches = 1

BATCHES = False
if BATCHES :
    for batch_df in np.array_split(df, num_batches):
        num_rows += batch_df.shape[0]
        if index == num_batches-1:
            isLast = True
        script.on_input(index,{'com.sap.headers.batch':[index,isLast,0,0,'unit']},batch_df)
        index +=1
else :
    script.on_input(index,{'com.sap.headers.batch':[index,True,0,0,'unit']},df)

logging.info("Load Model")
bstream = api.msg_list[0]['msg']
print(type(bstream))

model = pickle.load(bstream)

for p, v in model.get_params().items():
    print(f"{p}: {v}")

x_new = [[2005, 150, 80000]]
predictions = model.predict(x_new)
round(predictions[0][0], 2)