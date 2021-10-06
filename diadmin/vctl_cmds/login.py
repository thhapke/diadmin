#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#

# Using vctl
# URL: https://help.sap.com/viewer/0b99871a1d994d2ea89598fe59d16cf9/3.0.2/en-US/38f6d81551c44f5da0f10bd0249d67f1.html#loio38f6d81551c44f5da0f10bd0249d67f1

from subprocess import run, CalledProcessError
import logging

import yaml

# Login to di
def di_login(connection) :
    #logging.info(f"Login into \'{connection['URL']}\' as user \'{connection['USER']}\'")
    logging.info(f"Cmd: vctl login {connection['URL']} {connection['TENANT']} {connection['USER']} -p {connection['PWD']}")
    login_cmd = ['vctl','login',connection['URL'],connection['TENANT'],connection['USER'],'-p',connection['PWD']]
    ret = run(login_cmd)
    return ret.returncode

if __name__ == '__main__':

    with open('../../config_demo.yaml') as yamls:
        params = yaml.safe_load(yamls)

    #### LOGIN
    di_login(params)