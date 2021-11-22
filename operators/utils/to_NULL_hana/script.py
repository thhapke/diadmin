# DI-PYOPERATOR GENERATED - DO NOT CHANGE this line and the following 3 lines (Deleted when uploaded.)
#from utils.mock_di_api import mock_api
#api = mock_api(__file__)

import pandas as pd
import copy


def gen() :

    # Due to input-format PROPOSED transformation into DataFrame
    table_name = api.config.table_name
    sql_list = list()
    for col in api.config.num_columns :
        sql = f"UPDATE {table_name} SET \"{col}\" = NULL WHERE \"{col}\" =  {api.config.num_value}"
        sql_list.append(sql)
    for col in api.config.str_columns :
        sql = f"UPDATE {table_name} SET \"{col}\" = NULL WHERE \"{col}\" =  {api.config.str_value}"
        sql_list.append(sql)

    api.logger.info('SQL ' + '\n'.join(sql_list))
    for i,sql in enumerate(sql_list):
        api.logger.info(f'SQL message: {sql}')
        # Sending to outport output
        att = {'table_name':table_name}
        att['message.lastBatch'] = False
        if i == len(sql_list) - 1 :
            att['message.lastBatch'] = True
        out_msg = api.Message(attributes=att,body=sql)
        api.send('output',out_msg)    # datatype: message



api.add_generator(gen)

