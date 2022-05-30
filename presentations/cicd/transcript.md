# CI/CD with SAP Data Intelligence
## Introduction	
In the following short presentation I like to introduce you into a development operations  topic. How to do CI/CD or explicitly continuous innovation continuous deployment with SAP Data Intelligence.
After the presentation I am going into an SAP Data Intelligence system and I am showing you an example how a CICD process could look like. 

## One Tenant or Many Tenants
**Message:**  With proper setup 1 tenant is enough

First we should agree what kind of landscape we are using. To begin with we are using a cloud deployment. Then we have the options of either using a couple of tenants for each stage in the CICD process or  only one tenant on which we implement all stages. There are pros and cons for both approaches. If you like to have a strict separation of data sources and computational capacity then  having separate tenants might be the right choice. Nonetheless with a careful setup of policies, connections and resource quota for user you can implement the whole process using one tenant only. In the following I assume that we have a single tenant setup.

## User and Roles
**Message:**  For each DI user there is one separate workspace. You can create "Technical" user for special purposes and share the credentials among a group of users. The concept of impersonation that means user with their own credentials share a workspace is currently in discussion. 

Some words regarding the user workspaces. In SAP Data Intelligence each user has its own separate workspace in which he can develop and start pipelines that no other user have access to. Solelz in having  administration rights  development objects can be imported or exported and the pipelines be monitored.  There are cases like a test environment or the productive environment where many users should have access to. In this case you need to create a "Technical User" and share the credentials among the group of users who should have access to this workspace. An user impersonation is currently not supported where users could use their own credentials to access a shared environment. 

## Development Objects
What kind of development objects are produced with Data Intelligence?

Within SAP Data Intelligence you can basically develop 4 kinds of objects:		
- Dockerfiles, where you mostly add the unsupported packages to the docker images. 
- Virtual data types that you need for 2nd generation operators and pipelines to convey data between the operators
- Custom Operators
- Finally pipelines or graphs, how they are called sometimes

For more complex projects I recommend putting the code into packages that you can add using the dockerfiles to the docker images. For the external development you might use the development environment and test processes of you choice.
To my experience the average development time of the data intelligence objects is between hours and sometimes days. Each development object is developed by only one developer. The pipelines might use operators coming from different developers but again only one developer is charge for the object. A version control is always a benefit and therefore we have the plan deliver an official integration of git to the modeler. Right now we have versioning of operators in the modeler and a solution repository to which "committed" objects can be pushed with a manually given version. 
	
## CI/CD and Version Control 
**Message:** Different animals but there are overlaps or better git supports both. 

We have basically 2 development operations that are sometimes a bit mixed up therefore let me spend a few words on this. Thera are 

- CI/CD or Continuous Innovation and Continuous Deployment and 
- Version Control

The CI/CD process organizes the process from development to testing and to deployment of the applications. The version control manages the code changes. For this git has become a kind of standard and is widely used. You can use git of course also for CI/CD and together with automation tools like [Bamboo](https://www.atlassian.com/de/software/bamboo) or [Jenkins](https://www.jenkins.io/) you can replace the SAP Data Intelligence CI/CD process. And as soon as git will be integrated into the modeler UI it might even easier. 
	
Additionally we a currently considering if should not integrate our CI/CD to [SAP Cloud Application Lifecycle Management](https://support.sap.com/en/alm/sap-cloud-alm.html). So please watch out to any news about this. 

## Transportation Options Overview
After all the preparation we eventually come to the core topic of the presentation. We have the following transport option and let's start with the standard one

### System Management and Solution Repository
Each user, provided she has the authorization, can define in the application system managemen a solution and export it to the solution repository of the tenant. Other users can import the solution to their workspace. Each solution can encompass all kinds of development objects and has a version. 
	
### Export and Import of Files and Solutions to external repositories
The second option is to export a solution to a local file system either by using a function in the modeler UI or using again the system managment app, but this time use  "export as solution"  and not "export as solution to solution repository". Then you can store the solution to a git repository or any other storage from where other users can access the solution and import them to their user workspace. By this you can also share the solution with other tenants. 

## git-Integration
It is planned for the next release in Q3 2022 to have a direct push into a remote git repository from where other users can pull the objects to their workspace. By this we have  a full version control and can use git also for a transportation process. Nonetheless my recommendation is to stick with the CI/CD process that SAP Data Intelligence provides. 


# System Example Intro
Now let's have closer look to an example how to implement a transportation or CI/CD scenario. We have a team of developer who likes to develop an integration of the SAP Data Intelligence Catalog into an external business catalog. 

There are 3 teams:
- a developer team (Adana, Bao and Ciara) and each one of them has their own workspace
- a test team that share an user workspace and 
- an operations team running the productive pipelines

The central distribution hub is the solution repository to which every team member imports and exports their solutions. We have also a git repository to which each developer could push their versions. 

Let's focus on Bao's operational steps that highlights all the basic steps that all other projects members need to do as well. 

Bao's task is to use the operators Adana and Ciara had created and build a pipeline. Therefore he is 
1. importing the operators from the solution repository,
2. building the pipeline, 
3. saving the pipeline to a local and remote git repository and finally
4. exporting the solution consisting of all objects to the solution repository for the testing team to test. 

# System Demo
## Import Operators
Bao's first task is to download the operators that Adana and Ciara had developed and uploaded as solutions to the solution repository. He starts the System Adminstration App. As a developer he has not the data intelligence administrator authorizations and can only maintain his own user worksapce. 
Now he is checking if all operators had been succesfully imported. 

## Create Pipeline
Now Bao is openening the Modeler and is building a pipeline using the imported operators, is doing some configuration and eventually saves the result. 

## Saving to Github
Now Bao is changing to a local terminal and is starting a script that he has written, to download data intelligence objects. Furthermore the script is pushing the downloaded objects to a remote git repository. 
The script is rather simple and using the vctl - the command tool of SAP Data Intelligence. 

For copying the path to the object in the user workspace you like to export, you can use a menu function in the System Administration app.

## Export to Solution Repository
Finally Bao is bundling the metadata operators and his pipeline to a solution and exports the solution to the solution repository. He gives a name and version to the new solution. 
In the tenant tab you see all available solution including the newly created one. 