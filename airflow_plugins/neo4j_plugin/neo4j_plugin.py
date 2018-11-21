from airflow.plugins_manager import AirflowPlugin

from neo4j_plugin.operators.neo4j_operator import Neo4jOperator
from neo4j_plugin.hooks.neo4j_hook import Neo4jHook


class Neo4jPlugin(AirflowPlugin):
    name = 'neo4j_plugin'
    operators = [Neo4jOperator]
    hooks = [Neo4jHook]
