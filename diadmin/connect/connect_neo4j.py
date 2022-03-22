
from datetime import datetime
import logging

from neo4j import GraphDatabase

class neo4jConnection:

    def __init__(self, uri, user, pwd, db=None):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.db = db
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query,db):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session()
            response = list(session.run(query))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response

    def _eq_str(self,properties,variable='n',joined=', ',keys=None):
        prop_list = list()
        for p,v in properties.items() :
            if keys and not  p in keys :
                continue
            if type(v) == int or  type(v) == float :
                prop_list.append(f"{variable}.{p} = {str(v)}")
            elif type(v) == datetime :
                prop_list.append(f"{variable}.{p} = \'{v.isoformat()}\'")
            else :
                prop_list.append(f"{variable}.{p} = \'{v}\'")
        return joined.join(prop_list)

    def create_node(self,node):
        with self.__driver.session(database=self.db) as session:
            result = session.write_transaction(self._create_node, node)
            print(result)

    def _create_node(self,tx,node):
        properties = node['properties']
        label = node['label']
        prop_str = self._eq_str(properties)
        query = f'MERGE (n:{label}) SET {prop_str} RETURN n'
        result = tx.run(query)
        return result.single()[0]

    def create_relationship(self,relationship):
        with self.__driver.session(database=self.db) as session:
            result = session.write_transaction(self._create_relationship, relationship)
            print(result)

    def _create_relationship(self,tx,relationship):
        query = 'MATCH (n_from:{}), (n_to:{}) WHERE '.format(relationship['node_from']['label'],relationship['node_to']['label'])
        keysnf =  None if not 'keys' in relationship['node_from'] else relationship['node_from']['keys']
        keysnt =  None if not 'keys' in relationship['node_to'] else relationship['node_to']['keys']
        query += self._eq_str(relationship['node_from']['properties'],variable='n_from',keys=keysnf,joined=' AND ') + ' AND ' + \
                 self._eq_str(relationship['node_to']['properties'],variable='n_to',keys=keysnt,joined=' AND ') + ' '
        query += 'MERGE (n_from)-[:{}]->(n_to) RETURN n_from,n_to'.format(relationship['relation']['label'])
        result = tx.run(query)
        logging.info("Relationship: {result}")

if __name__ == "__main__":


    gdb = neo4jConnection("bolt://localhost:7687", "neo4j", "meta4Graph",db='metadata')
    node_tenant = {'label':'TENANT',
            'properties':{'tenant':'default','url':'https://vsystem.ingress.dh-93awrk8fy.dh-canary.shoot.live.k8s-hana.ondemand.com'}}
    gdb.create_node(node_tenant)
    node_connection = {'label':'CONNECTION',
                       'properties':{'id':'TEST_ID','type':'TEST_TYPE','description':'Only for test','n': 4.89,'created':datetime.now()},
                       'keys':['id']}
    gdb.create_node(node_connection)
    relationship = {'node_from':node_tenant,'node_to':node_connection,'relation':{'label':'HAS_CONNECTION'}}
    gdb.create_relationship(relationship)

    #gdb.add_connection({'id':'DI_DEMO','type':'WASB','description':'DI_DEMO'})
    #gdb.close()