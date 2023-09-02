from web3 import Web3
import pprint
import json

def attributeDict_to_json(attrDict):
    return json.loads(json.dumps(eval(str(attrDict)[14:-1])))
def HexBytes(hex):
    return hex
w3 = Web3(Web3.HTTPProvider('http://localhost:8545'))
print(w3.isConnected())
print(w3.eth.syncing)
print(w3.eth.block_number)
# blockInfo = attributeDict_to_json(w3.eth.syncing)
# pprint.pprint(blockInfo)
print()
block_2000000 = attributeDict_to_json(w3.eth.get_block(125347))
pprint.pprint(block_2000000)
print(block_2000000['hash'])
tx_sample = attributeDict_to_json(w3.eth.get_transaction_by_block(10000000,0))
tx_sample2 = attributeDict_to_json(w3.eth.get_transaction_by_block(12531115, 165))
tx_sample3 = attributeDict_to_json(w3.eth.get_transaction_by_block(12534783 , 112))
tx_sample4 = attributeDict_to_json(w3.eth.get_transaction('0x5721977c1389057dc55c0931dcac46746ea48be9d783e02ce7c581a2130784b7'))
# tx_sample4 = attributeDict_to_json(w3.eth.get_transaction_by_block(2, 0))
# print(tx_sample)
# pprint.pprint(tx_sample)
# print(w3.eth.get_transaction('0x5dbb0835079497df591b96ee44be67b841daadce03de6fdef3234a83133c25e7'))


tx4 = w3.eth.get_transaction_receipt('0x3706905552f19de9da6eebad2ed17e79031d438f2a4510c133e6958cdb7023d0')
print(tx4['gasUsed']*tx_sample2['gasPrice'])

