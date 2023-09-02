import asyncio
from genericpath import isdir
from aiohttp import ClientSession
from web3.providers.base import JSONBaseProvider
from web3.providers import HTTPProvider
from web3.exceptions import *
from web3 import Web3
import toml
import os
import time
from py2neo.bulk import create_relationships
from py2neo.bulk import create_nodes
from py2neo.matching import *
from py2neo import *
import logging
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
import timeit
import sys


def HexBytes(hex):
    return hex


def DecimalToHex(dec):
    digits = "0123456789ABCDEF"
    x = (dec % 16)
    rest = dec // 16
    if rest == 0:
        return digits[x]
    return DecimalToHex(rest) + digits[x]


def toHex(dec):
    return '0x{}'.format(DecimalToHex(dec))

# asynchronous JSON RPC API request
async def async_make_request(session, url, method, params):
    base_provider = JSONBaseProvider()
    request_data = base_provider.encode_rpc_request(method, params)
    async with session.post(url, data=request_data,
                            headers={'Content-Type': 'application/json'}) as response:
        content = await response.read()
    response = base_provider.decode_rpc_response(content)
    return response


async def run(getHttp, blockNum):
    tasks = []

    # Fetch all responses within one Client session,
    # keep connection alive for all requests.
    async with ClientSession() as session:
        task = asyncio.ensure_future(async_make_request(session, getHttp,
                                                        'eth_getBlockByNumber', [toHex(blockNum), True]))
        tasks.append(task)

        responses = await asyncio.gather(*tasks)
        # print(responses)
    return responses


class transaction_crawler:

    def __init__(self):
        setting = toml.load(f'{os.getcwd()}/setting.toml')
        self.from_block = setting['init']['from_block']
        self.block_logger = self.logger_config(log_path=f'{os.getcwd()}/log.txt', logging_name='CryptoGraph')
        neo4j = setting['neo4j']
        self.gethHttp = setting['gethHttpServer']['url']
        self.graph = Graph(neo4j['url'], auth=(neo4j['username'], neo4j['password']))
        self.block_logger.info('Connected to neo4j server {}'.format(neo4j['url']))
        self.w3 = Web3(Web3.HTTPProvider(self.gethHttp))
        self.block_logger.info('Connected to geth http server {}'.format(self.gethHttp))
        self.batch_unit = setting['init']['batch_unit']


    def start_crawl(self):
        current_block = self.from_block - 1
        batch = list()
        while True:
            current_block += 1
            if current_block <= self.getHighestBlockNumber():
                self.block_logger.info('Parsing block #{}'.format(current_block))
                try:
                    loop = asyncio.get_event_loop()
                    future = asyncio.ensure_future(run(self.gethHttp, current_block))
                    responses = loop.run_until_complete(future)
                    batch.append(responses)
                    if len(batch) == self.batch_unit:
                        self.process_responses(batch, 'ETH')
                        batch = list()
                except TransactionNotFound:
                    pass
            else:
                current_block -= 1
                time.sleep(2)
                

    def getHighestBlockNumber(self):
        return self.w3.eth.block_number

    def checkNode(self, node):
        nodeMatcher = NodeMatcher(self.graph)
        return nodeMatcher.match('addr',id=node).first()


    def process_responses(self,batch, asset):
        node_set = set()
        rel_list = list()
        for responses in batch:
            for jsonrpc in responses:
                block = jsonrpc['result']
                node_keys = ['id']
                for tx in block['transactions']:
                    new_tx = {'blockNumber': int(tx['blockNumber'], 16), 'hash': tx['hash'], 'value': str(int(tx['value'], 16)),
                            'from': tx['from'], 'to': tx['to'], 'asset': asset, 'timestamp': str(int(block['timestamp'], 16)),
                            'transaction_fee': self.generate_tx_fee(tx['hash'], tx['gasPrice'])}
                    if new_tx['from'] is None:
                        new_tx['from'] = 'invalid'

                    if new_tx['to'] is None:
                        new_tx['to'] = 'invalid'

                    from_addr = new_tx['from']
                    # if nodeMatcher.match('addr',id=from_addr).first() is None:
                    node_set.add(from_addr)
                    to_addr = new_tx['to']
                    # if nodeMatcher.match('addr',id=to_addr).first() is None:
                    node_set.add(to_addr)
                    new_tx.pop('from')
                    new_tx.pop('to')
                    rel_list.append((from_addr,new_tx,to_addr))

        all_tasks = []
        with ThreadPoolExecutor(max_workers=15) as executor:
            for node in node_set:
                task = executor.submit(self.checkNode, node)
                all_tasks.append(task)

        result_list = []
        for future in as_completed(all_tasks):
            result = future.result()
            result_list.append(result)

        node_exist = set()
        for re in result_list:
            if re is not None:
                node_exist.add(re['id'])
        node_set = node_set - node_exist
        
        if len(node_set) != 0:
            node_list = [[address] for address in node_set]
            create_nodes(tx=self.graph.auto(), data=node_list, labels={"addr"}, keys=node_keys)
            self.block_logger.info('node')
        create_relationships(tx=self.graph.auto(), data=rel_list, rel_type="tx", start_node_key=("addr","id"), end_node_key=("addr","id"))
        self.block_logger.info('tx')

        

    def generate_tx_fee(self, tx_hash, gasPrice):
        if gasPrice == '':
            return 'none'
        try:
            tx1 = self.w3.eth.get_transaction_receipt(tx_hash)
            return str(tx1['gasUsed'] * gasPrice)
        except TransactionNotFound:
            return 'none'

    def generate_timestamp(self, blockNum):
        try:
            block = self.w3.eth.get_block(blockNum)
            return block['timestamp']
        except BlockNotFound:
            return 'none'

    def load_large_obj(self, input_file_path):
        bytes_in = bytearray(0)
        max_bytes = 2 ** 31 - 1
        input_size = os.path.getsize(input_file_path)
        with open(input_file_path, 'rb') as f_in:
            for i in range(0, input_size, max_bytes):
                size = min(max_bytes, input_size - i)
                bytes_in += f_in.read(size)
        obj = pickle.loads(bytes_in)
        print("Finish loading the dict")
        return obj

    def store_large_obj(self, obj, output_file_path):
        max_bytes = 2 ** 31 - 1
        bytes_out = pickle.dumps(obj)
        with open(output_file_path, 'wb') as f_out:
            for idx in range(0, len(bytes_out), max_bytes):
                size = min(max_bytes, len(bytes_out) - idx)
                f_out.write(bytes_out[idx:idx + size])
        print("Finish storing the dict")

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


txCrawler = transaction_crawler()
txCrawler.start_crawl()
