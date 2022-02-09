import sys
import time
from datetime import datetime
import logging
from os.path import dirname, join, abspath
proj_dir = join(dirname(dirname(dirname(dirname(abspath(__file__))))))
sys.path.insert(0, proj_dir)

import script

from diadmin.dimockapi.mock_api import api
from diadmin.dimockapi.mock_inport import operator_test

optest = operator_test(__file__)

# config parameter
api.config.errorHandling = '{"type":"terminate on error"}' # type: string
api.config.num_rows = 5 # type: integer
api.config.periodicity = 1.0 # type: number
api.config.max_index = 5.0 # type: number
api.num_crashes = 2

logging.basicConfig(level=logging.INFO)

num_retry = 2
epoch = None
snapshot = None

while script.index < api.config.max_index:
    logging.info(f"call gen: {script.index}")
    try :
        sleep_time = script.gen()
    except ValueError as ve:
        logging.error(ve)
        if epoch :
            logging.info(f"Restore snapshot: {epoch}")
            api.restore(epoch,snapshot)

    logging.info("Take snapshot")
    epoch = datetime.utcnow().strftime("YYYY-%mm-%ddT%HH:%MM:%SS.%f")
    snapshot = api.serialize(epoch) # epoch: 2022-01-23T08:10:29.901Z

    logging.info(f"Sleep for {sleep_time} ")
    time.sleep(sleep_time)


for m in api.msg_list:
    print(m)