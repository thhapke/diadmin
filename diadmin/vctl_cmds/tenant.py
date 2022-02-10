#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
import logging
import json
from subprocess import check_output


def get_info(tenant_name,format = 'json') :
    cmd = ['vctl','tenant','get',tenant_name,'--format','json']
    output_str = check_output(cmd).decode('utf-8')
    output_dict = json.loads(output_str)
    if not format == 'json' :
        cmd = ['vctl','tenant','get',tenant_name,'--format',format]
        output_str = check_output(cmd).decode('utf-8')
    return output_dict, output_str