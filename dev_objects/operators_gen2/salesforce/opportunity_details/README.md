# SalesForce Opportunity Details

Operator calls RestAPI to retrieve the opportunity details. Due to the host of passwords and keys two HTTP-connections in the Connection Management are used.

1st http connection:
- host
- port
- protocol
- path
- Basic Authentication
- User, to authenticate to the RestAPI
- Password

2nd http connection: 
Only using 
- User, space-separated:  <CallerGuid> <CallerKey> <apikey> <apikeysecret>
- Password, <Proxy-Authorization>


# I/O 

### Inport: Input

Opportunity ID: String

### Outport:Output

1-row table with the details of the opportunity: salesforce.opportunity

## Configuration

Territory: String

Parameter for the RestAPI. 


