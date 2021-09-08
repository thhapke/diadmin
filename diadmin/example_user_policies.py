import yaml
import json
import csv
from subprocess import run, check_output

# Read config yaml for the login details
with open('config.yaml') as yamls:
    params = yaml.safe_load(yamls)

# login
login_cmd = ['vctl','login',params['URL'],params['TENANT'],params['USER'],'-p',params['PWD']]
run(login_cmd)

list_user_assignments_cmd = ['vctl','policy','list-assignments','-a','--format','json']
list_user_assignments_str = check_output(list_user_assignments_cmd)
list_user_assignments = json.loads(list_user_assignments_str)

# Saving the policy to file
with open("user_assignments.json", 'w') as json_file:
    json.dump(list_user_assignments,json_file,indent=4)

# Converting to csv because it is a flat json
with open("user_assignments.csv", 'w') as csv_file:
    fwriter = csv.writer(csv_file)
    fwriter.writerow(list_user_assignments[0].keys())
    for ua in list_user_assignments :
        fwriter.writerow(ua.values())
