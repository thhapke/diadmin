# Mock apis needs to be commented before used within SAP Data Intelligence
#from diadmin.dimockapi.mock_api import mock_api
#api = mock_api(__file__)


import copy
import pandas as pd
import numpy as np
import io

from scipy.spatial import KDTree
from geopy.distance import geodesic


def on_input(msg_net,msg_data):

	net_dim1 = api.config.dim_net[0]
	net_dim2 = api.config.dim_net[1]
	data_dim1 = api.config.dim_data[0]
	data_dim2 = api.config.dim_data[1]

	### INPUT templates
	# Input message.file to DataFrame
	df_net = pd.read_csv(io.BytesIO(msg_net.body))
	#df_net = df_net[[api.config.id_net,net_dim1,net_dim2]]

	# Input message.table to DataFrame
	header = [c['name'] for c in msg_data.attributes['table']['columns']]
	df_data = pd.DataFrame(msg_data.body, columns=header)

	tree = KDTree(np.c_[df_net[net_dim1].ravel(),df_net[net_dim2].ravel()])
	nodes_p = np.vstack((df_data[data_dim1],df_data[data_dim2])).transpose()
	_ ,df_data['NN_INDEX'] = tree.query(nodes_p,k=1)
	suffixes = [api.config.suffix_data,api.config.suffix_net]
	df = pd.merge(df_data,df_net,how='inner',left_on='NN_INDEX',right_index=True,suffixes=suffixes)
	df.drop(columns=['NN_INDEX'],inplace=True)

	data2_dim1 = data_dim1 + api.config.suffix_data
	data2_dim2 = data_dim2 + api.config.suffix_data
	net_dim1 += api.config.suffix_net
	net_dim2 += api.config.suffix_net
	def distance(row) :
		return geodesic((row[data2_dim1],row[data2_dim2]),(row[net_dim1],row[net_dim2])).km

	df['DISTANCE'] = df.apply(distance,axis=1)
	df['FROM_DATE'] = pd.to_datetime(df['FROM_DATE'])
	df.drop(columns = ['TO_DATE'],inplace=True)

	#df.drop(columns=[net_dim1,net_dim2],inplace=True)
	#df.rename(columns={data2_dim1:data_dim1,data2_dim2:data_dim2},inplace = True)

	### OUTPUT templates
	# Output DataFrame to message.table
	att = copy.deepcopy(msg_net.attributes)
	att['header'] = df.columns.tolist()
	dtype_map = {'int64':'integer','float64':'float','object':'string','datetime64[ns]':'timestamp'}
	col_types = { col:dtype_map[dt.name] for col,dt in df.dtypes.items() }
	table_dict = {'version':1,'columns':list(),'name':'table'}
	table_dict['columns'] = [{'name':col,'class': col_types[col].lower()} for col in df.columns ]
	att['table'] = table_dict
	data = df.values.tolist()


	msg_output = api.Message(attributes=att,body=data)
	api.send('output',msg_output)  # data type: message.table


api.set_port_callback(['net','data'],on_input)