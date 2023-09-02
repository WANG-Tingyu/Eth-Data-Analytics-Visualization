from py2neo import *
from py2neo.matching import *

graph = Graph('bolt://127.0.0.1:7687', auth=('neo4j', 'nft'))
print('abc')
# nodes = NodeMatcher(graph)
# matcher = nodes.match("thetaToken")
# for node in matcher:
#     print(node)

# cypher_edges = "MATCH ()-[r:transfer_to]->() " + \
#                            "RETURN r"
# tx_list = graph.run(cypher_edges)
# for tx in tx_list:
#     print(tx)

node_cypher = "MATCH (b) " + \
                    "WHERE id(b) = 121391868 " + \
                    "RETURN b.id"
query_result = graph.run(node_cypher)
query_result.forward()
print('node: {}'.format(query_result.current))