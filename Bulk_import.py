import toml
import os
import subprocess
import time
import sys
from py2neo import *
from py2neo.bulk import create_relationships
from py2neo.bulk import create_nodes
from py2neo.matching import *
from datetime import datetime
import requests
import logging
import _pickle as pickle
from itertools import islice
import hashlib


def load_large_obj(input_file_path):
    bytes_in = bytearray(0)
    max_bytes = 2**31 - 1
    input_size = os.path.getsize(input_file_path)
    with open(input_file_path, 'rb') as f_in:
        for i in range(0, input_size, max_bytes):
            size = min(max_bytes, input_size-i)
            bytes_in += f_in.read(size)
    obj = pickle.loads(bytes_in)
    print("Finish loading the sequence")
    return obj

class transaction_crawler:
    def __init__(self):
        setting = toml.load(f'{os.getcwd()}/setting.toml')
        neo4j = setting['neo4j']
        self.tx_lists = setting['init']['tx_lists']
        self.datadir = setting['init']['datadir']
        self.graph = Graph(neo4j['url'], auth=(neo4j['username'], neo4j['password']))
        self.block_logger = self.logger_config(log_path=f'{os.getcwd()}/log.txt', logging_name='ETH')
        

    def start_crawl(self):
        # transactions = self.getTransactions(blockHeight)
        addr_set = set()
        for tx_list in self.tx_lists:
            transactions = load_large_obj(f'{self.datadir}{tx_list}')
            # only consider usdt with 'simple send' type
            for tx in transactions:
                    addr_set.add(tx['from'])
                    addr_set.add(tx['to'])
        print('number of nodes: {}'.format(len(addr_set)))
        self.BulkImportNodes(addr_set)
        addr_set = set()

        for tx_list in self.tx_lists:
            transactions = load_large_obj(f'{self.datadir}{tx_list}')

            self.BulkImportTx(transactions)

    def BulkImportNodes(self, addr_set):
        node_keys = ['id']
        node_list = [[address] for address in addr_set]    
        stream = iter(node_list)
        cnt = 0
        while True:
            batch = islice(stream, 200000)
            batch_list = [elem for elem in batch]
            if len(batch_list) != 0:
                cnt = cnt + len(batch_list)
                print('start addr {}'.format(cnt))
                create_nodes(tx=self.graph.auto(), data=batch_list, labels={"addr"}, keys=node_keys)
                print('finish addr {}'.format(cnt))
            else:
                break

    def BulkImportTx(self, transactions):
        cnt = 0
        stream = iter(transactions)
        while True:
            tx_data = list()
            batch = islice(stream, 1000000)
            batch_list = [elem for elem in batch]
            if len(batch_list) != 0:
                cnt = cnt + len(batch_list)
                print('start tx {}'.format(cnt))
                for tx in batch_list:
                    tx_rel = (tx['from'], {'blockNumber': tx['blockNumber'], 'hash': tx['hash'], 'value': tx['value'],
                       'asset': 'ETH', 'timestamp': tx['timestamp']}, tx['to'])
                    tx_data.append(tx_rel)
                create_relationships(tx=self.graph.auto(), data=tx_data, rel_type="tx", start_node_key=("addr","id"), end_node_key=("addr","id"))
                print('finish tx {}'.format(cnt))
            else:
                break

    
    def logger_config(self, log_path, logging_name):
        '''
        config log
        :param log_path: output log path
        :param logging_name: record name，optional
        :return:
        '''

        logger = logging.getLogger(logging_name)
        logger.setLevel(level=logging.DEBUG)

        handler = logging.FileHandler(log_path, encoding='UTF-8')
        handler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)

        logger.addHandler(handler)
        logger.addHandler(console)
        return logger


crawl = transaction_crawler()
crawl.start_crawl()
