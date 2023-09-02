import toml
import os
from py2neo import *
from py2neo.database import Schema
from py2neo.bulk import create_relationships
from py2neo.bulk import create_nodes
from py2neo.matching import *

setting = toml.load(f'{os.getcwd()}/setting.toml')
neo4j = setting['neo4j']
graph = Graph(neo4j['url'], auth=(neo4j['username'], neo4j['password']))
schema = Schema(graph)
# schema.drop_index('addr','id')
print('connect to the server')
schema.create_index('addr','id')
print('finish addr indexing')
# # ---------------------------------------------------
# nodes = NodeMatcher(graph)
# a = nodes.match("tx", hash="d71263de3851c4815ca06964b34f2a8a9da9da2259292f6d5b5978d2119fd6e9").first()
# b = nodes.match("addr", id="DdzFFzCqrhsqz23SkTxevzJ3Dn4ee14BpQVe5T9LX2yWJpcjHToP2qxnzaEiy5qiHwNVtX5ANXtLJyBwKz8PvjJZYq2n8fyy7Dp9RqXa").first()
# a = Node("tx", hash="e21036e892aa66fe6c367c704ac1892ca4684661577cb46ab5969ff2ec036683")
# print(b)
# ------------------------------------------------
# graph.delete_all()