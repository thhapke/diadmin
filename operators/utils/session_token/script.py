# Mock apis needs to be commented before used within SAP Data Intelligence
#from diadmin.dimockapi.mock_api import mock_api
#api = mock_api(__file__)

import os
import json
import requests
import http.client
from base64 import b64encode


def gen():

    app_id = os.environ.get("VSYSTEM_APP_ID")
    user = os.environ.get('VSYSTEM_USER')
    secret = os.environ.get("VSYSTEM_SECRET")
    tenant = os.environ.get('VSYSTEM_TENANT')

    #api.send('logging',"Environment: {}".format(os.environ))

    if not user :
        user = 'teched21'
    if not tenant :
        tenant = 'default'
    if not secret :
        secret = api.config.password
    if not app_id :
        app_id = user
        
    OPTION1 = False        
    if OPTION1 :

        headers = {'X-Requested-With': 'XMLHttpRequest'}
        auth=(app_id,secret)
        api.send('logging',f'Requests connection with {auth}' )
        r = requests.get(url='http://vsystem-internal:8796/token/v2',auth=auth,headers=headers)
        try :
            response = json.loads(r.text)
            data = f"{response}"
        except ValueError as ve:
            response = "No RESPONSE"
            api.send('logging',f'JSON Decoding Error: {ve}')
        
        if r.status_code == 200 :
    
            api.send('logging',f'Requests successful: {response}')
            api.send('token',data)  # data type: string
            att = {'user': user,
                   'app_id': app_id,
                   'tenant':tenant,
                   'secret':secret}
            msg = api.Message(attributes=att,body = data)
            api.send('output',msg)  # data type: string
        else:
            api.send('logging',f'Response status: {r.status_code}')
        
    
        att = {'user':user,
               'tenant':tenant,
               'password':api.config.password}
    
        msg = api.Message(attributes=att,body = None)
        api.send('output',msg)  # data type: string
        
    else :
        # Get bearer token
        userAndPass = b64encode(bytes(app_id + ':' + secret, "utf-8")).decode("ascii")
        headers = { 'Authorization' : 'Basic %s' %  userAndPass }

        conn = http.client.HTTPConnection('vsystem-internal','8796')
        conn.request("GET","/token/v2", headers=headers)

        # Put auth bearer token into header
        r = conn.getresponse()

        if r.status == 200 :
            api.send('logging','HTTPconnection connection')
            responseText = json.loads(r.read())
            bearerHeader = { 'Authorization' : responseText['access_token'] }
            data = f"{bearerHeader}"
            att = {'user': user,
                   'app_id': app_id,
                   'tenant':tenant,
                   'secret':secret}
            msg = api.Message(attributes=att,body = data)
            api.send('output',msg)  # data type: string
        else :
            api.send('logging',f'Response status: {r.status}')
        

api.add_generator(gen)