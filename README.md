<!--
SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>

SPDX-License-Identifier: Apache-2.0
-->

# SAP Data Intelligence Admin  Tools

This tools help me to manage the user and the security of my DEMO Data Intelligence systems. The pre-requiste is to first install vctl (System Management Command-line) that can be dowloaded from the [SAP Download Center](https://launchpad.support.sap.com/#/softwarecenter/template/products/%20_APP=00200682500000001943&_EVENT=DISPHIER&HEADER=Y&FUNCTIONBAR=N&EVENT=TREE&NE=NAVIGATE&ENR=73554900100800002981&V=INST&TA=ACTUAL&PAGE=SEARCH/DATA%20INTELLIGENCE-SYS%20MGMT%20CLI). 

## Command-line scripts

### dipolicy 

Command line script that supports admin tasks regarding policy managment, like 

* downloading and 
* uploading policies including their resources
* analyses policy dependency and producing a 
  * csv-file of policy resources
  * visualizes  policy network

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
didownload --help 
usage: didownload [-h] [-c CONFIG] [-n SOLUTION] [-v VERSION] [-u USER] [-g] {operators,pipelines,dockerfiles,solution} artifact

Downloads operators, pipelines or solution to local from SAP Data Intelligence to local file system. Pre-requiste: vctl.

positional arguments:
  {operators,pipelines,dockerfiles,solution}
                        Type of artifacts.
  artifact              Artifact name (package, graph or dockerfile)

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        Specifies yaml-config file
  -n SOLUTION, --solution SOLUTION
                        Solution imported to vrep before artifacts downloaded.
  -v VERSION, --version VERSION
                        If artifact is of solution type then version is needed
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
usage: diupload [-h] [-c CONFIG] [-r CONFLICT] [-n SOLUTION] [-s DESCRIPTION] [-v VERSION] [-u USER] {operators,pipelines,dockerfiles,bundle} artifact

Uploads operators, pipelines, dockerfiles and bundle to SAP Data Intelligence. Pre-requiste: vctl.

positional arguments:
  {operators,pipelines,dockerfiles,bundle}
                        Type of artifacts. 'bundle'- only supports .tgz-files with differnt artifact types.
  artifact              Artifact file(tgz) or directory

optional arguments:
  -h, --help            show this help message and exit
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

```



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

