#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#

import logging
import re
import yaml
from os.path import join, basename,isfile,dirname, relpath
import os
import argparse
import boto3
from botocore.exceptions import NoCredentialsError

def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Upload to S3")
    parser.add_argument('S3connect', help = 'Credentials and path of S3')
    parser.add_argument('source', help = 'Source to be uploaded to S3')
    parser.add_argument('target', help = 'Target path at s3.')
    parser.add_argument('-t','--test', help='Test without uploading',action='store_true')
    parser.add_argument('-m','--maxfiles', type=int,help='Maximum number of files to upload.',default=10000)

    args = parser.parse_args()

    if args.S3connect:
        config_file = args.S3connect
        if not re.match('.+\.yaml',config_file):
            config_file += '.yaml'
    with open(config_file) as yamls:
        params = yaml.safe_load(yamls)

    access_key = params['ACCESS_KEY']
    secret_key = params['SECRET_KEY']
    bucket =  params['BUCKET']

    MAX_FILES = args.maxfiles

    if args.test :
        logging.warning('Only testing without uploading files.')
    if args.maxfiles < 100000 :
        logging.warning(f'Maximum number files to be uploaded: {args.maxfiles}.')

    logging.info('Create S3 client')
    s3 = boto3.client('s3', aws_access_key_id=access_key,aws_secret_access_key=secret_key)

    if isfile(args.source):
        logging.info(f'Upload file: {args.source}')
        target_file = join(args.target,basename(args.source))
        if not args.test :
            s3.upload_file(args.source, bucket, target_file)
        logging.info(f"Upload Successful: {args.source} to {target_file}")

    else :
        logging.info(f'Upload files of directory: {args.source}')
        count = 0
        dir = dirname(args.source)
        for root, dirs, files in os.walk(args.source):
            for f in files:
                rdir = re.sub(dir,'',root)
                if rdir[0]==os.sep :
                    rdir = rdir[1:]
                target_file = join(args.target,rdir,basename(f))
                if not args.test:
                    s3.upload_file(f, bucket, target_file)
                logging.info(f"Upload Successful: {f} to {target_file}")
                if count == MAX_FILES-1 :
                    logging.warning(f'Maximum of upload files reached: {MAX_FILES}')
                    break
                count +=1


if __name__ == '__main__':
    main()