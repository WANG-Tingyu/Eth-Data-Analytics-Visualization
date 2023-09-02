from py2neo import *
from py2neo.matching import *

graph = Graph('bolt://127.0.0.1:7687', auth=('neo4j', 'nft'))


edge_exist_cypher_format = "MATCH ()-[r:{0}]->() " + \
                            "WHERE r.hash='{1}' " + \
                            "RETURN r"


cypher_sum = 'match (n) return count(n), sum ( size( (n)-[]->()))'
print(graph.run(cypher_sum))