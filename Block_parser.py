import asyncio
from aiohttp import ClientSession
from web3.providers.base import JSONBaseProvider
from web3.providers import HTTPProvider
from web3 import Web3
import toml
import os
import pickle


def DecimalToHex(dec):
    digits = "0123456789ABCDEF"
    x = (dec % 16)
    rest = dec // 16
    if rest == 0:
        return digits[x]
    return DecimalToHex(rest) + digits[x]


def toHex(dec):
    return '0x{}'.format(DecimalToHex(dec))


def store_large_obj(obj, output_file_path):
    max_bytes = 2 ** 31 - 1
    bytes_out = pickle.dumps(obj)
    with open(output_file_path, 'wb') as f_out:
        for idx in range(0, len(bytes_out), max_bytes):
            size = min(max_bytes, len(bytes_out) - idx)
            f_out.write(bytes_out[idx:idx + size])
    print("Finish storing the obj")


def load_large_obj(input_file_path):
    bytes_in = bytearray(0)
    max_bytes = 2 ** 31 - 1
    input_size = os.path.getsize(input_file_path)
    with open(input_file_path, 'rb') as f_in:
        for i in range(0, input_size, max_bytes):
            size = min(max_bytes, input_size - i)
            bytes_in += f_in.read(size)
    obj = pickle.loads(bytes_in)
    print("Finish loading the obj")
    return obj


def process_responses(addr_set, tx_list, responses, asset):
    for jsonrpc in responses:
        block = jsonrpc['result']
        for tx in block['transactions']:
            new_tx = {'blockNumber': int(tx['blockNumber'], 16), 'hash': tx['hash'], 'value': str(int(tx['value'], 16)),
                      'from': tx['from'], 'to': tx['to'], 'asset': asset, 'timestamp': str(int(block['timestamp'], 16))}
            tx_list.append(new_tx)
            addr_set.add(new_tx['from'])
            addr_set.add(new_tx['to'])


# asynchronous JSON RPC API request
async def async_make_request(session, url, method, params):
    base_provider = JSONBaseProvider()
    request_data = base_provider.encode_rpc_request(method, params)
    async with session.post(url, data=request_data,
                            headers={'Content-Type': 'application/json'}) as response:
        content = await response.read()
    response = base_provider.decode_rpc_response(content)
    return response


async def run(getHttp, blockNums):
    tasks = []

    # Fetch all responses within one Client session,
    # keep connection alive for all requests.
    async with ClientSession() as session:
        for blockNum in blockNums:
            task = asyncio.ensure_future(async_make_request(session, getHttp,
                                                            'eth_getBlockByNumber', [toHex(blockNum), True]))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)
        # print(responses)
    return responses


if __name__ == "__main__":
    setting = toml.load(f'{os.getcwd()}/setting.toml')
    gethHttp = setting['gethHttpServer']['url']
    # [from_block, to_block] inclusive
    from_block = int(setting['init']['from_block'])
    to_block = int(setting['init']['to_block'])
    datadir = setting['init']['datadir']
    addr_set = set()
    tx_list = list()
    left = from_block
    right = from_block + 499999
    lastBatch = False

    while from_block <= to_block:
        blockNums = []
        if to_block - from_block <= 10:
            blockNums = [from_block + i for i in range(to_block - from_block + 1)]
            lastBatch = True
        else:
            blockNums = [from_block + i for i in range(10)]
        print(f'block# {str(blockNums[0])} to {str(blockNums[len(blockNums) - 1])}')
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(run(gethHttp, blockNums))
        responses = loop.run_until_complete(future)
        process_responses(addr_set, tx_list, responses, 'ETH')
        if (from_block + 9) % 500000 == 0:
            store_large_obj(addr_set, f'{datadir}addr_set_{left}_{right}')
            store_large_obj(tx_list, f'{datadir}tx_list_{left}_{right}')
            addr_set = set()
            tx_list = list()
            left = from_block + 10
            right = from_block + 10 + 499999
        elif lastBatch:
            store_large_obj(addr_set, f'{datadir}addr_set_{left}_{to_block}')
            store_large_obj(tx_list, f'{datadir}tx_list_{left}_{to_block}')
            break
        from_block += 10
