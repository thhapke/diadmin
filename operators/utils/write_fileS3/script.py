# Mock apis needs to be commented before used within SAP Data Intelligence
#from diadmin.dimockapi.mock_api import mock_api
#api = mock_api(__file__)

from os.path import join,split
import os
import json
import boto3
from boto3.session import Session

session = None


def on_input(msg):
    global session

    api.logger.info(f'Operator config: {json.dumps(api.config.connectionS3,indent=4)}')

    if api.config.session and  session == None:
        session = api.logger.info('Log into S3 using \'session\'')
        client = boto3.client('S3',
                              aws_access_key_id=api.config.connectionS3["connectionProperties"]["accessKey"],
                              aws_secret_access_key=api.config.connectionS3["connectionProperties"]["secretKey"],
                              region_name=api.config.connectionS3["connectionProperties"]["region"])
        response = client.assume_role(RoleArn=api.config.role, RoleSessionName='di_s3')
        session = Session(aws_access_key_id=response['Credentials']['AccessKeyId'],
                          aws_secret_access_key=response['Credentials']['SecretAccessKey'],
                          aws_session_token=response['Credentials']['SessionToken'])

    if api.config.session :
        client = session.client("s3")
    else :
        api.logger.info('Create S3 Client')
        client = boto3.client('s3',
                              aws_access_key_id=api.config.connectionS3["connectionProperties"]["accessKey"],
                              aws_secret_access_key=api.config.connectionS3["connectionProperties"]["secretKey"])


    rp =api.config.connectionS3["connectionProperties"]["rootPath"].split(os.sep,1)
    target = join(rp[1],api.config.path) if api.config.path else rp[1]
    target = join(target,msg.attributes['filename'])
    client.put_object(Body = msg.body, Bucket = rp[0],Key = target)

    
api.set_port_callback("fileInput",on_input)



