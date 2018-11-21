from airflow.hooks.base_hook import BaseHook

from neo4j.v1 import GraphDatabase


class Neo4jHook(BaseHook):
    """
    The Neo4j Python driver is officially supported by Neo4j and
    connects to the database using the binary protocol.
    It aims to be minimal, while being idiomatic to Python.
    pip install neo4j-driver
    https://neo4j.com/developer/python/#neo4j-python-driver
    You can specify connection string options in extra field of your connection
    https://neo4j.com/docs/api/python-driver/current/
    """
    conn_type = 'Neo4j'

    def __init__(self, neo4j_conn_id='neo4j_default', *args, **kwargs):
        super().__init__(source='neo4j')
        self.neo4j_conn_id = neo4j_conn_id
        self.neo4j_connection = self.get_connection(neo4j_conn_id)
        self.neo4j_driver = None

    def get_driver(self):
        """
        Fetches neo4j driver
        """
        if not self.neo4j_driver:
            conn = self.neo4j_connection
            host = conn.host
            port = conn.port
            uri = str(host) + ":" + str(port)
            user = conn.login
            password = conn.password
            self.neo4j_driver = GraphDatabase.driver(uri, auth=(user, password))
        return self.neo4j_driver

    def get_session(self):
        """
        Fetches neo4j session
        """
        neo4j_driver = self.get_driver()
        return neo4j_driver.session()

    def execute_cql(self, cql):
        """
        Executes cql
        """
        if not cql:
            return
        tx = self.get_session().begin_transaction()
        if isinstance(cql, list):
            cql_sequence = cql
        else:
            cql_sequence = [cql]
        for cql in cql_sequence:
            tx.run(cql)
        tx.commit()

    def on_kill(self):
        """
        Gets neo4j connection closed
        """
        if self.neo4j_driver:
            self.neo4j_driver.close()
            self.neo4j_driver = None
