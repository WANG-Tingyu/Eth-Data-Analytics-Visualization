from py2neo import *
from py2neo.matching import *

graph = Graph('bolt://127.0.0.1:7687', auth=('neo4j', 'nft'))

cypher = "MATCH (n) " + \
         "DETACH DELETE n"
graph.run(cypher)
print("Reset complete")
