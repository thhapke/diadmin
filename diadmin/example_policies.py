

import yaml
import json
from subprocess import run, check_output

# Read config yaml for the login details
with open('config.yaml') as yamls:
    params = yaml.safe_load(yamls)

# login
login_cmd = ['vctl','login',params['URL'],params['TENANT'],params['USER'],'-p',params['PWD']]
run(login_cmd)

# Get list policies
list_policies_cmd = ['vctl','policy','list-policies','--format','json']
list_policies_str = check_output(list_policies_cmd).decode('utf-8')
policies = json.loads(list_policies_str)

# Get details of each policy
policy_details_list = list()
for p in policies :
    print(f"Get policy details of {p['id']}")
    policies_cmd = ['vctl','policy','get',p['id'],'--format','json']
    policy_detail_str = check_output(policies_cmd).decode('utf-8')
    policy_details_list.append(json.loads(policy_detail_str))

# add integer id to each policy for easing later analysis
policy_details_list = [ {**p,'num_id' : i+1 } for i,p in enumerate(policy_details_list)]

# Saving the policy to file
with open("policy_list.json", 'w') as json_file:
    json.dump(policy_details_list,json_file,indent=4)