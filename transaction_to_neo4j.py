from py2neo import *
from py2neo.bulk import create_relationships
from py2neo.bulk import create_nodes
import traceback

class neo4jAdapter:
    def __init__(self, url, username, password):
        self.graph = Graph(url, auth=(username, password))

    def create_node(self, addr, tokenType):
        node_cypher_format = "CREATE (" + \
                             "n:{0} " + \
                             "{{id:'{1}'}}) " + \
                             "RETURN id(n)"

        cypher = ''
        try:
            cypher = node_cypher_format.format(tokenType, addr)
            node_result = self.graph.run(cypher)
            node_result.forward()
            # print('node created: (id){}'.format(node_result.current))
            return str(node_result.current)
        except:
            print(cypher)
            traceback.print_exc()

    def create_edge(self, tx, aid, bid, tokenType):
        edge_cypher_format = "MATCH (a), (b) " + \
                             "WHERE id(a) = {0} AND id(b) = {1} " + \
                             f"SET a:{tokenType}, b:{tokenType} " + \
                             "CREATE (a)-[t:transfer_to {2}]->(b) " + \
                             "RETURN t.hash"

        cypher = ''
        try:
            cypher = edge_cypher_format.format(aid, bid,
                                               self.tx_preprocess(str(tx)))
            edge_result = self.graph.run(cypher)
            edge_result.forward()
            # print('edge created: (hash){}'.format(edge_result.current))
        except:
            print(cypher)
            traceback.print_exc()

    def bulk_create_nodes(self, data, labels):
        keys = ['id']
        create_nodes(tx=self.graph.auto(), data=data, labels=labels, keys=keys)

    def bulk_create_relationships(self, data):
        create_relationships(tx=self.graph.auto(), data=data, rel_type="transfer_to")



    def add_a_tokenType(self, accountID, tokenType):
        """
        Add a token type to those accounts who participant in more than one NTF

        :param accountID:
        :param tokenType:
        :return: no return
        """
        cypher = f"MATCH (n {{id: '{accountID}'}}) " + \
                 f"SET n:{tokenType} " + \
                 "RETURN n.id"
        try:
            result = self.graph.run(cypher)
            result.forward()
            # print('label({}) added: (id){}'.format(tokenType, result.current))
        except:
            print('error to run ' + cypher)
            traceback.print_exc()
            return True

    def tx_preprocess(self, tx):
        """
        Remove the quotation marks on key to satisfy the syntax of CQL

        :param tx:
        :return: tx without the quotation marks on keys
        """

        stack = []
        quotation_pos = []
        tx_list = [c for c in tx]
        for pos in range(len(tx)):
            if tx[pos] == '\'':
                stack.append(pos)
            elif tx[pos] == ':':
                quotation_pos.append(stack.pop())
                quotation_pos.append(stack.pop())

        for pos in quotation_pos:
            tx_list[pos] = '#'

        processed_tx = ''.join(tx_list)
        return processed_tx.replace('#', '')

# adapter = neo4jAdapter('bolt://192.168.55.13:7687', 'neo4j', 'nft')
# tx1 = "{'blockHash': '0xaf0615219cf8b66cabdd0ca559cc27dfc070740489f5f83fd7afcdf717d00ee4', 'blockNumber': 60003, 'from': '0x86889b1CeA13d370E827e099e3778b17d488B3Dd', 'gas': 500000, 'gasPrice': 59234020395, 'hash': '0xf417de45ccef630ffad3ddc88dbf40aacfe9f29391df01dd77721964c4574e1c', 'input': '0x', 'nonce': 1, 'to': '0x109C4f2CCc82C4d77Bde15F306707320294Aea3F', 'transactionIndex': 1, 'value': 1000000000000000000, 'type': '0x0', 'v': 27, 'r': '0x8497a84410ae061c6a83f73f0a317cf95a6b0f2821187a477a02e1dd9497c251', 's': '0x486ac75b7c7ed15c6f4fd4c670de32f1d6a03e03f8bd3ce44b228d43b2b00b95'}"
# tx2 = "{'blockHash': '0xaf0615219cf8b66cabdd0ca559cc27dfc070740489f5f83fd7afcdf717d00ee4', 'blockNumber': 60003, 'from': '0x3D0768da09CE77d25e2d998E6a7b6eD4b9116c2D', 'gas': 21000, 'gasPrice': 59088242020, 'hash': '0x09ac27043afe74a1dc237da78ff5d130a937fc328596785b2cfee4c3a4fdf3d7', 'input': '0x', 'nonce': 37, 'to': '0x37fdD436C4F06bbD63a93dA53EC3Ea91645F8866', 'transactionIndex': 2, 'value': 999000000000000000, 'type': '0x0', 'v': 28, 'r': '0x34fad980dcefeec6d9eeadf04eb7e764c97051f1178ef3fa27cbb954bdf5da54', 's': '0x20235cbe464ff13c0840d3ae3f058da3579b8b5f981be956e582e7a063758940'}"
# tx3 = "{'blockHash': '0xaf0615219cf8b66cabdd0ca559cc27dfc070740489f5f83fd7afcdf717d00ee4', 'blockNumber': 60003, 'from': '0x667f01423a28c172E9593779554952B0c2c6524D', 'gas': 21000, 'gasPrice': 57768233563, 'hash': '0xb24d20c2641703c30c3de0f09f2220f8b7e4458885b4fc19b72e5ea8daf15bf2', 'input': '0x', 'nonce': 0, 'to': '0x32Be343B94f860124dC4fEe278FDCBD38C102D88', 'transactionIndex': 3, 'value': 1993775010000000000, 'type': '0x0', 'v': 28, 'r': '0x206dfd83afcfaa5447bab18a8a1739e95dda46897aaf46d03a9122a6f048664c', 's': '0x402da3658893e12fa78e3c46824105459e4402e1f5e02955ff67bd170ac3a0c1'}"
# tx4 = "{'blockHash': '0xaf0615219cf8b66cabdd0ca559cc27dfc070740489f5f83fd7afcdf717d00ee4', 'blockNumber': 60003, 'from': '0x667f01423a28c172E9593779554952B0c2c6524D', 'gas': 21000, 'gasPrice': 57768233563, 'hash': '0x4e4d20c2641703c30c3de0f09f2220f8b7e4458885b4fc19b72e5ea8daf15bf2', 'input': '0x', 'nonce': 0, 'to': '0x32Be343B94f860124dC4fEe278FDCBD38C102D88', 'transactionIndex': 3, 'value': 1993775010000000000, 'type': '0x0', 'v': 28, 'r': '0x206dfd83afcfaa5447bab18a8a1739e95dda46897aaf46d03a9122a6f048664c', 's': '0x402da3658893e12fa78e3c46824105459e4402e1f5e02955ff67bd170ac3a0c1'}"

# adapter.transaction_parse(eval(tx1), 'thetaToken')
# adapter.transaction_parse(eval(tx2), 'thetaToken')
# adapter.transaction_parse(eval(tx1), 'thetaToken')
# adapter.transaction_parse(eval(tx3), 'thetaToken')
# adapter.transaction_parse(eval(tx4), 'ENJ')
# # adapter.create_edge(eval(tx4),'ENJ')
# a = adapter.create_node('0x86889b1CeA13d370E827e099e3778be7d488B3cd', 'SAND')
# print(type(str(a)))
# print(a)