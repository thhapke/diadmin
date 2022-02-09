from datetime import datetime, timezone
import string

import pandas as pd
import numpy as np

class data_generator:

    alphabeth = list(string.ascii_lowercase)

    def __init__(self,num_rows,str_len=5) :
        self.data = None
        self.batch = -1
        self.str_len = str_len
        self.num_rows = num_rows
        self.crashed_already = False
        self.start_time = datetime.now()

    def next(self) :
        df = pd.DataFrame(index=range(self.num_rows),columns=['batch','id','timestamp','string_value','float_value'])
        df['batch'] = self.batch
        df['id'] = df.index
        df['timestamp'] = datetime.now(timezone.utc)
        df['string_value'] = df['string_value'].apply(lambda x: ''.join(np.random.choice(data_generator.alphabeth, size=self.str_len)))
        df['float_value'] =  np.random.uniform(low=0., high=1000, size=(self.num_rows,))
        self.batch += 1
        self.data = df
        
    def running_time(self) :
        return (datetime.now() - self.start_time).total_seconds()//60

    def serialize(self):
        return (self.batch,self.data,self.crashed_already)

    def restore(self,rdata) :
        self.batch = rdata[0]
        self.data = rdata[1]
        self.crashed_already = rdata[2]
