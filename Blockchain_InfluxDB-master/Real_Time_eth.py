import toml
import os
from aiohttp import ClientSession
from web3.providers.base import JSONBaseProvider
from web3.providers import HTTPProvider
from web3.exceptions import *
from web3 import Web3
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import time
import logging
import asyncio


def DecimalToHex(dec):
    digits = "0123456789ABCDEF"
    x = (dec % 16)
    rest = dec // 16
    if rest == 0:
        return digits[x]
    return DecimalToHex(rest) + digits[x]


def toHex(dec):
    return '0x{}'.format(DecimalToHex(dec))

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


class Real_Time_to_InfluxDB:
    def __init__(self):
        setting = toml.load(f'{os.getcwd()}/setting.toml')

        self.from_block = setting['init']['from_block']

        self.block_logger = self.logger_config(log_path=f'{os.getcwd()}/eth_block_log.txt', logging_name='CryptoGraph')

        self.gethHttp = setting['gethHttpServer']['url']
        influxDB = setting['InfluxDB']
        self.bucket = influxDB['bucket']
        self.org = influxDB['org']
        client = InfluxDBClient(url=influxDB['url'], token=influxDB['token'])
        self.write_api = client.write_api(write_options=SYNCHRONOUS)
        self.block_logger.info('Connected to InfluxDB server {}'.format(influxDB['url']))
        self.w3 = Web3(Web3.HTTPProvider(self.gethHttp))
        self.block_logger.info('Connected to geth http server {}'.format(self.gethHttp))

    def start(self):
        current_block = self.from_block - 1
        while True:
            current_block += 1
            if current_block <= self.getHighestBlockNumber():
                
                loop = asyncio.get_event_loop()
                future = asyncio.ensure_future(run(self.gethHttp, current_block))
                responses = loop.run_until_complete(future)
                data = self.process_responses(responses)
                # print(data)
                try:
                    self.write_api.write(self.bucket, self.org, data)
                except:
                    time.sleep(1)
                    self.write_api.write(self.bucket, self.org, data)
                self.block_logger.info('Finish block #{}'.format(current_block))
            else:
                print(f'wait for block #{current_block}')
                current_block -= 1
                time.sleep(15)

    def getHighestBlockNumber(self):
        return self.w3.eth.block_number

    def process_responses(self, responses):
        for jsonrpc in responses:
            block = jsonrpc['result']
            accumulateValue = 0
            for tx in block['transactions']:
                accumulateValue += int(tx['value'], 16)
            blkHeader = {'blockNumber': int(block['number'], 16), 'totalValue': accumulateValue,
                        'timestamp': str(int(block['timestamp'], 16)), 'gasUsed':str(int(block['gasUsed'], 16)),
                        'size':str(int(block['size'], 16)), 'txs':len(block['transactions'])}
            nanotimestamp = str(blkHeader['timestamp']) + '000000000'
            return "block,crypto=eth blockNumber={},totalValue={},gasUsed={},size={},txs={} {}".format(str(blkHeader['blockNumber']),str(blkHeader['totalValue']),str(blkHeader['gasUsed']),str(blkHeader['size']),str(blkHeader['txs']), nanotimestamp)


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
        

influxDB = Real_Time_to_InfluxDB()
influxDB.start()