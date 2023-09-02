from datetime import datetime
from datetime import timezone

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import pickle
import os
import time

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


def insertBlocks(sequence, blockHeader_list):
    for blkHeader in blockHeader_list:
        nanotimastamp = str(blkHeader['timestamp']) + '000000000'
        data = "block,crypto=eth blockNumber={},totalValue={},gasUsed={},size={},txs={} {}".format(str(blkHeader['blockNumber']),str(blkHeader['totalValue']),str(blkHeader['gasUsed']),str(blkHeader['size']),str(blkHeader['txs']), nanotimastamp)
        sequence.append(data)

    
            

# You can generate a Token from the "Tokens Tab" in the UI
token = "RqUzXWOp2SO7tc3OExwcpDApUHWrGssC-7ByqramD9YmvJTkelMnysxIW5Vz-Lay6eVJZpvKWrRIwsOvifd05g=="
org = "hkbu"
bucket = "CryptoBlocks"

client = InfluxDBClient(url="http://localhost:8086", token=token)

blockHeader_list = load_large_obj(f'../data/blockHeader_1_12000000')
write_api = client.write_api(write_options=SYNCHRONOUS)
sequence = list()
insertBlocks(sequence, blockHeader_list)
blockHeader_list = []




for i in range(0, len(sequence), 100000):
    size = min(1000, len(sequence) - i)
    write_api.write(bucket, org, sequence[i:i+size])
    print('finish {} to {}'.format(i, i+size))


