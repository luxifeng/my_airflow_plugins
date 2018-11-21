from airflow.models import BaseOperator
from airflow.utils import apply_defaults

from neo4j_plugin.hooks.neo4j_hook import Neo4jHook


class Neo4jOperator(BaseOperator):
    @apply_defaults
    def __init__(self, cql, neo4j_conn_id='neo4j_default', *args, **kwargs):
        super(Neo4jOperator, self).__init__(*args, **kwargs)
        self.cql = cql
        self.neo4j_conn_id = neo4j_conn_id
        self.neo4j_hook = None

    def execute(self, context):
        """
        Executed by task instance at runtime
        """
        self.log.info("Execute: %s", self.cql)
        self.neo4j_hook = Neo4jHook(neo4j_conn_id=self.neo4j_conn_id)
        if not self.cql:
            raise Exception("missing required argument 'cql'")
        self.neo4j_hook.execute_cql(cql=self.cql)

    def on_kill(self):
        """
        Exected when task instance gets killed
        """
        if self.neo4j_hook:
            self.neo4j_hook.on_kill()
            self.neo4j_hook = None
        self.log.info("Closed: %s", self.neo4j_conn_id)