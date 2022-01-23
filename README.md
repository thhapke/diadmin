<!--
SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>

SPDX-License-Identifier: Apache-2.0
-->`

# diadmin - SAP Data Intelligence Admin Tools 

Commandline tool with Python packages that helps me to run my operation tasks for the SAP Data Intelligence Cloud instances. Most of the commands use vctl and some official RestAPIs ([SAP API Data Hub](https://api.sap.com/package/SAPDataIntelligenceCloud/overview)) and unoffical RestAPIs.

**Attention**: This is a private and unsupported solution. Of course I am happy to get hints on bugs and try to solve them. 

## Pre-requiste 
System Management Command-line of SAP Data Intelligence (vctl)

Download: [SAP Download Center](https://launchpad.support.sap.com/#/softwarecenter/template/products/%20_APP=00200682500000001943&_EVENT=DISPHIER&HEADER=Y&FUNCTIONBAR=N&EVENT=TREE&NE=NAVIGATE&ENR=73554900100800002981&V=INST&TA=ACTUAL&PAGE=SEARCH/DATA%20INTELLIGENCE-SYS%20MGMT%20CLI). 


## Installation

```
pip diadmin
```

## Summary

### Commandline

All commands use a configuration yaml-file (option: --config) that at least needs the URL and the credentials of the SAP Data Intelligence system:

```
TENANT: default
URL: https://vsystem.ingress.xxx.shoot.live.k8s-hana.ondemand.com
USER: user
PWD: pwd123
```
Some commands need more configuration parameters. Each command comes with a help option (--help)

| Command | Description | Config Parameter | API type|
|---|---|:---:|:---:|
|dibackup | Downloads some DI artificats (operators, pipelines, dockerfiles, solutions) to local folders.| - | vctl|
|didownload <type> <artifact>| Downloads the specified artifacts (operators,graphs,dockerfiles,general) to local folder. Wildcards supported.| - | vctl|
|diupload <type> <artifact>| Uploads the specified artifacts in the local folder to DI.| - | vctl|
|diconnections | Downloads the connections (Uploaded option open).| - | metadata api|
|dimock| Creates a script.py template out of operator.json and configSchema.json including a local test-script for offline development. Uses dimockapi package. | - |-|
|dipolicy | Downloads, uploads and analyses DI policies.| RESOURCE_CLASSES, COLOR_MAP, POLICY_FILTER,CLASS_THRESHOLD| vctl|
|diuser | Downloads user, creates new user, deletes user, assignes policies to user ..|USERLISTS, USER_ROLE| vctl| 
|dicatalog <type> <item> | Downloads and uploads catalog hierarchies and dataset tags. Additionally downloads connections and container (=data source path) | - | metadata api| 
|dipmonitor | Downloads the runtime pipeline information of user| - | runtime api|
|didockerbuild <dockerfile path> | Starts docker build of Dockerfile for user | - | private api|
|dipipelinesbatch <json-batch-file> | Starts pipelines from batch with maximum number of running pipelines. | - | runtime api|

### Packages

- **dimockapi** Creating script templates based on operator.json and configSchema.json and a test-script for offline testing. In addtion it contains a mock_api package.
- **metadata_api** Using the metadata RestAPIs of SAP API Business Hub 
- **utils** Collection of helper functions
- **vctl_cmds** Python wrapper around vctl-commands
- **analysis** For analysing the policy data

## Details 

### dipolicy 

Command line script that supports admin tasks regarding policy managment, like 

- **diupload**: uploading development artifacts (operators, graphs, dockerfiles, menus, solutions)
- **didownload**: downloading development artifacts (operators, graphs, dockerfiles, menus, solutions)
* analyses policy dependency and producing a 
  * csv-file of policy resources
  * visualizes  policy network
* export and import policies
* build docker images in user workspaces
* creating user in Data Intelligence system with defined roles/policies
* monitors pipelines
* creates a custom operator script framework using config.json and operatorSchema.json

Reads policy data from SAP Data Intelligence and provides a policy network, chart and a resources.csv file for further analysis.

```
usage: dipolicy [-h] [-c CONFIG] [-g] [-d DOWNLOAD] [-u UPLOAD] [-f FILE] [-a]

"Policy utility script for SAP Data Intelligence. Pre-requiste: vctl. "

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        Specifies yaml-config file
  -g, --generate        Generates config.yaml file
  -d DOWNLOAD, --download DOWNLOAD
                        Downloads specified policy. If 'all' then all policies are download
  -u UPLOAD, --upload UPLOAD
                        Uploads new policy.
  -f FILE, --file FILE  File to analyse policy structure. If not given all policies are newly downloaded.
  -a, --analyse         Analyses the policy structure. Resource list is saved as 'resources.csv'.

```

### dipmonitor

List of pipelines user has started recently. Needs a config.yaml with SAP Data Intelligence credentials:

```
URL : 'https://vsystem.ingress.myinstance.ondemand.com'
TENANT: 'default'
USER : 'user'
PWD : 'pwd123'
```


### didownload - part of diadmin

Downloads SAP Data Intelligence artifacts 

* operators
* pipelines
* Dockerfiles

to local files systems in order to be offline modified or tested (operators) or using a local git implementation for a version control. The script as to started from the root folder of the project that has the following structure: 

project/ 
* operators/
  * package/
    * (optional) subpackage/
      * operator/
        * operator-files
        * ...
* pipelines
  * package/
    * pipeline/
      * pipeline-file with sub-folders
* dockerfiles
  * name of dockerfile
    * Dockerfile
    * Tags.json

In the root folder a config.yaml file is needed. With the option ```--config``` you can specify which config-file should be used in case e.g. you work with different user or SAP Data Intelligence instances. The basic parameters of config.yaml are

```
URL : 'https://vsystem.ingress.myinstance.ondemand.com'
TENANT: 'default'
USER : 'user'
PWD : 'pwd123'

```

The ```--help``` option describes the additional options
```
ddidownload --help
usage: didownload [-h] [-c CONFIG] [-i] [-n SOLUTION] [-v VERSION] [-u USER] [-g] {operators,graphs,dockerfiles,all,*,solution} artifact

Downloads operators, pipelines or solution to local from SAP Data Intelligence to local file system. Pre-requiste: vctl.

positional arguments:
  {operators,graphs,dockerfiles,all,*,solution}
                        Type of artifacts.
  artifact              Artifact name of package, graph or dockerfile or wildcard '*'. For 'all' wildcard is required.

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        Specifies yaml-config file
  -i, --init            Creates a config.yaml and the necessary folders. Additionally you need to add '* *' as dummy positional arguments
  -n SOLUTION, --solution SOLUTION
                        Solution imported to vrep before artifacts downloaded.
  -v VERSION, --version VERSION
                        Version of solution. Required for option --solution
  -u USER, --user USER  SAP Data Intelligence user if different from login-user. Not applicable for solutions-download
  -g, --gitcommit       Git commit for the downloaded files

```

### diupload - part of diadmin

Uploads locally stored SAP Data Intelligence artifacts

* operators
* pipelines
* Dockerfiles

to an SAP Data Intelligence instance. The usage is similar to ```didownload``` that uses the same project structure and ```config.yaml``` file.

The ```--help``` option describes the additional options

```
diupload --help 
usage: diupload [-h] [-i] [-c CONFIG] [-r CONFLICT] [-n SOLUTION] [-s DESCRIPTION] [-v VERSION] [-u USER] [-g] {operators,graphs,dockerfiles,all,*} artifact

Uploads operators, graphs, dockerfiles and bundle to SAP Data Intelligence. Pre-requiste: vctl.

positional arguments:
  {operators,graphs,dockerfiles,all,*}
                        Type of artifacts. 'bundle'- only supports .tgz-files with differnt artifact types.
  artifact              Artifact file(tgz) or directory

optional arguments:
  -h, --help            show this help message and exit
  -i, --init            Creates a config.yaml and the necessary folders. Additionally you need to add '* *' as dummy positional arguments
  -c CONFIG, --config CONFIG
                        Specifies yaml-config file
  -r CONFLICT, --conflict CONFLICT
                        Conflict handling flag of 'vctl vrep import'
  -n SOLUTION, --solution SOLUTION
                        Solution name if uploaded artificats should be exported to solution repository as well.
  -s DESCRIPTION, --description DESCRIPTION
                        Description string for solution.
  -v VERSION, --version VERSION
                        Version of solution. Necessary if exported to solution repository.
  -u USER, --user USER  SAP Data Intelligence user if different from login-user. Not applicable for solutions-upload
  -g, --gitcommit       Git commit for the uploaded files


```

### dimock - part of diadmin

Builds a framework of a new python script for a custom operator. 

dimock --help
usage: dimock [-h] [-w] operator

Prepare script for offline development

positional arguments:
operator         Operator folder

optional arguments:
-h, --help       show this help message and exit
-w, --overwrite  Forcefully overwrite existing script



### Additional Modules in diadmin Package

### genpwds 

####  genpwd
    Generate password with a given length with ascii excluding ambigiuos characters
    :param len_pwd: Passeword length (default 8)
    :return: password

#### gen_user_pwd_list
    Generates a generic user-password list with a given user prefix. Used for workshops
    :param num_user: Number of users (default 10)
    :param len_pwd: Lenght of password (default 8)
    :param prefix: User prefix (default user_
    :return: dictionary (dict[user]=pwd

### useradmin

Contains functions for 

* creating user lists
* sychronizing local user list with SAP Data Intelligence user, 
* Assigning and deassigning policies for user

`