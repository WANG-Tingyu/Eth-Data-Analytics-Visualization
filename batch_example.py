import timeit
import asyncio

from aiohttp import ClientSession

from web3.providers.base import JSONBaseProvider
from web3.providers import HTTPProvider
from web3 import Web3

# synchronously request receipts for given transactions
def sync_receipts(web3, transactions):
    for tran in transactions:
        web3.eth.getTransactionReceipt(tran)

# asynchronous JSON RPC API request
async def async_make_request(session, url, method, params):
    base_provider = JSONBaseProvider()
    request_data = base_provider.encode_rpc_request(method, params)
    async with session.post(url, data=request_data,
                        headers={'Content-Type': 'application/json'}) as response:
        content = await response.read()
    response = base_provider.decode_rpc_response(content)
    return response

async def run(node_address, transactions):
    tasks = []

    # Fetch all responses within one Client session,
    # keep connection alive for all requests.
    async with ClientSession() as session:
        for tran in transactions:
            task = asyncio.ensure_future(async_make_request(session, node_address,
                                                            'eth_getTransactionReceipt',[tran.hex()]))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)

if __name__ == "__main__":
    eth_node_address = "http://localhost:8546"
    web3 = Web3(HTTPProvider(eth_node_address))

    block = web3.eth.getBlock(10000000)
    transactions = block['transactions']

    start_time = timeit.default_timer()
    sync_receipts(web3, transactions)
    print('sync: {:.3f}s'.format(timeit.default_timer() - start_time))

    start_time = timeit.default_timer()
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(eth_node_address, transactions))
    loop.run_until_complete(future)
    print('async: {:.3f}s'.format(timeit.default_timer() - start_time))