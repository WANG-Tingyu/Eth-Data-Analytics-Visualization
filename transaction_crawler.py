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
from transaction_to_neo4j import neo4jAdapter
from alchemy_request import Alchemy_request
import logging
import pickle
import timeit


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


class CircleIterator:
    def __init__(self, length):
        self.currentValue = -1
        self.length = length

    def next(self):
        if self.currentValue + 1 >= self.length:
            self.currentValue = 0
        else:
            self.currentValue += 1
        return self.currentValue


class transaction_crawler:

    def __init__(self):
        setting = toml.load(f'{os.getcwd()}/setting.toml')
        self.alchemy_urls = setting['api_key']['url']
        self.from_block = setting['init']['from_block']
        self.token_blocks_per_call = setting['init']['token_blocks_per_call']
        self.token_contract_address = setting['token_contract_address']
        self.circleIterator = CircleIterator(len(self.alchemy_urls))
        self.block_logger = self.logger_config(log_path=f'{os.getcwd()}/block_log.txt', logging_name='CryptoGraph')
        self.dict_logger = self.logger_config(log_path=f'{os.getcwd()}/dict_log.txt', logging_name='CryptoAddress')
        neo4j = setting['neo4j']
        self.gethHttp = setting['gethHttpServer']['url']
        self.neo4jTxAdapter = neo4jAdapter(neo4j['url'], neo4j['username'], neo4j['password'])
        self.block_logger.info('Connected to neo4j server {}'.format(neo4j['url']))
        self.w3 = Web3(Web3.HTTPProvider(self.gethHttp))
        self.block_logger.info('Connected to geth http server {}'.format(self.gethHttp))
        self.addr_dict = self.load_large_obj(f'{os.getcwd()}/addr_dict.p')
        self.nft_tx_count = 0

    def start_crawl(self):
        current_block = self.from_block - 1
        nft_start_block = self.from_block
        checkpoint4dict = 0
        towait = False
        while True:
            current_block += 1
            checkpoint4dict += 1
            if current_block <= self.getHighestBlockNumber():
                self.block_logger.info('Parsing block #{}'.format(current_block))
                try:
                    loop = asyncio.get_event_loop()
                    future = asyncio.ensure_future(run(self.gethHttp, current_block))
                    responses = loop.run_until_complete(future)
                    self.process_responses(responses, 'ETH')
                except TransactionNotFound:
                    pass
            else:
                checkpoint4dict -= 1
                current_block -= 1
                towait = True
                
            # if (
            #         current_block - nft_start_block) % self.token_blocks_per_call == 0 and current_block - nft_start_block != 0 and self.nft_tx_count >= 1:
            #     self.alchemy_call(nft_start_block, current_block)
            #     nft_start_block = current_block + 1
            #     self.nft_tx_count = 0
            #     towait = False

            if checkpoint4dict % 6000 == 0:
                self.store_large_obj(self.addr_dict, f'{os.getcwd()}/addr_dict.p')
                self.dict_logger.info('Check at Block #{} (inclusive)'.format(current_block))
                checkpoint4dict = 0
                towait = False
            if towait:
                print(f'wait for block #{current_block}')
                towait = False
                time.sleep(15)

    def getHighestBlockNumber(self):
        return self.w3.eth.block_number

    def identifyNFTs(self, to_address):
        try:
            for contract_address in self.token_contract_address.values():
                if contract_address.lower() == to_address.lower():
                    return True
            return False
        except AttributeError:
            return False

    def process_transaction(self, tx, asset, timestamp):
        new_tx = {'blockNumber': tx['blockNumber'], 'hash': tx['hash'], 'value': str(tx['value']),
                  'from': tx['from'], 'to': tx['to'], 'asset': asset, 'timestamp': timestamp,
                  'transaction_fee': self.generate_tx_fee(tx['hash'], tx['gasPrice'])}
     
        self.decorate_and_upload(new_tx, asset)

    def process_responses(self,responses, asset):
        # start_time = timeit.default_timer()
        dict_highest_idx = len(self.addr_dict) - 1
        node_list = list()
        rel_list = list()
        for jsonrpc in responses:
            block = jsonrpc['result']
            for tx in block['transactions']:
                new_tx = {'blockNumber': int(tx['blockNumber'], 16), 'hash': tx['hash'], 'value': str(int(tx['value'], 16)),
                        'from': tx['from'], 'to': tx['to'], 'asset': asset, 'timestamp': str(int(block['timestamp'], 16)),
                        'transaction_fee': self.generate_tx_fee(tx['hash'], tx['gasPrice'])}
                if new_tx['from'] is None:
                    new_tx['from'] = 'invalid'

                if new_tx['to'] is None:
                    new_tx['to'] = 'invalid'

                if new_tx['from'] not in self.addr_dict.keys():
                    # id = self.neo4jTxAdapter.create_node(new_tx['from'], asset)
                    dict_highest_idx += 1
                    self.addr_dict[new_tx['from']] = dict_highest_idx
                    node_list.append([new_tx['from']])

                if new_tx['to'] not in self.addr_dict.keys():
                    # id = self.neo4jTxAdapter.create_node(new_tx['to'], asset)
                    dict_highest_idx += 1
                    self.addr_dict[new_tx['to']] = dict_highest_idx
                    node_list.append([new_tx['to']])

                # if self.identifyNFTs(new_tx['to']):
                #     self.nft_tx_count += 1
                triple = (int(self.addr_dict[new_tx['from']]), new_tx, int(self.addr_dict[new_tx['to']]))
                rel_list.append(triple)
                # self.decorate_and_upload(new_tx,asset)
        
        self.neo4jTxAdapter.bulk_create_nodes(node_list, {"ETH"})
        self.neo4jTxAdapter.bulk_create_relationships(rel_list)
        # print('One Block: {:.3f}s'.format(timeit.default_timer() - start_time))
        

            
    def decorate_and_upload(self, new_tx, asset):
        if new_tx['from'] is None:
            new_tx['from'] = 'invalid'

        if new_tx['to'] is None:
            new_tx['to'] = 'invalid'

        if new_tx['from'] not in self.addr_dict.keys():
            start_time = timeit.default_timer()
            self.addr_dict[new_tx['from']] = self.neo4jTxAdapter.create_node(new_tx['from'], asset)
            print('from node: {:.3f}s'.format(timeit.default_timer() - start_time))

        if new_tx['to'] not in self.addr_dict.keys():
            start_time = timeit.default_timer()
            self.addr_dict[new_tx['to']] = self.neo4jTxAdapter.create_node(new_tx['to'], asset)
            print('to node: {:.3f}s'.format(timeit.default_timer() - start_time))

        start_time = timeit.default_timer()
        self.neo4jTxAdapter.create_edge(new_tx, self.addr_dict[new_tx['from']], self.addr_dict[new_tx['to']], asset)
        print('tx: {:.3f}s'.format(timeit.default_timer() - start_time))

    def alchemy_call(self, from_block, to_block):
        self.block_logger.info(f'Making alchemy call from block #{from_block} to block #{to_block}')
        alchemy_request = Alchemy_request(self, from_block, to_block, self.token_contract_address, self.alchemy_urls,
                                          self.circleIterator.next())
        tx_list = alchemy_request.make()
        for tx in tx_list:
            try:
                temp = self.w3.eth.get_transaction(tx['hash'])
                tx['gasPrice'] = temp['gasPrice']
            except TransactionNotFound:
                tx['gasPrice'] = ''
            tx['blockNumber'] = int(tx['blockNum'], 16)
            print(tx)
            self.process_transaction(tx, tx['asset'], self.generate_timestamp(tx['blockNumber']))

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
