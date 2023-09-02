import pickle
import os
import csv

def load_large_obj(input_file_path):
    bytes_in = bytearray(0)
    max_bytes = 2**31 - 1
    input_size = os.path.getsize(input_file_path)
    with open(input_file_path, 'rb') as f_in:
        for i in range(0, input_size, max_bytes):
            size = min(max_bytes, input_size-i)
            bytes_in += f_in.read(size)
    obj = pickle.loads(bytes_in)
    print("Finish loading the set")
    return obj

def store_large_obj(obj, output_file_path):
    max_bytes = 2**31 - 1
    bytes_out = pickle.dumps(obj)
    with open(output_file_path, 'wb') as f_out:
        for idx in range(0, len(bytes_out), max_bytes):
            size = min(max_bytes, len(bytes_out)-idx)
            f_out.write(bytes_out[idx:idx+size])
    print("Finish storing the set")

def merge(set1, set_file_path):
    set2 = load_large_obj(set_file_path)
    return set1 | set2

set1 = load_large_obj('/data/mingxing/EthBlockToNeo4j/addr_set_1_1000000')
set1 = merge(set1,'/data/mingxing/EthBlockToNeo4j/addr_set_1000001_2000000')
set1 = merge(set1,'/data/mingxing/EthBlockToNeo4j/addr_set_2000001_3000000')
set1 = merge(set1,'/data/mingxing/EthBlockToNeo4j/addr_set_3000001_4000000')
set1 = merge(set1,'/data/mingxing/EthBlockToNeo4j/addr_set_4000001_5000000')
set1 = merge(set1,'/data/mingxing/EthBlockToNeo4j/addr_set_5000001_6000000')
set1 = merge(set1,'/data/mingxing/EthBlockToNeo4j/addr_set_6000001_7000000')
set1 = merge(set1,'/data/mingxing/EthBlockToNeo4j/addr_set_7000001_7500000')
set1 = merge(set1,'/data/mingxing/EthBlockToNeo4j/addr_set_7500001_8000000')
set1 = merge(set1,'/data/mingxing/EthBlockToNeo4j/addr_set_8000001_8500000')
set1 = merge(set1,'/data/mingxing/EthBlockToNeo4j/addr_set_8500001_9000000')
set1 = merge(set1,'/data/mingxing/EthBlockToNeo4j/addr_set_9000001_9500000')
set1 = merge(set1,'/data/mingxing/EthBlockToNeo4j/addr_set_9500001_10000000')
set1 = merge(set1,'/data/mingxing/EthBlockToNeo4j/addr_set_10000001_10500000')
set1 = merge(set1,'/data/mingxing/EthBlockToNeo4j/addr_set_10500001_11000000')
set1 = merge(set1,'/data/mingxing/EthBlockToNeo4j/addr_set_11000001_11500000')
set1 = merge(set1,'/data/mingxing/EthBlockToNeo4j/addr_set_11500001_12000000')
set1 = merge(set1,'/data/mingxing/EthBlockToNeo4j/addr_set_12000001_12500000')
set1 = merge(set1,'/data/mingxing/EthBlockToNeo4j/addr_set_12500001_12595900')
set1 = merge(set1,'/data/mingxing/EthBlockToNeo4j/addr_set_12595901_12597940')
set1 = merge(set1,'/data/mingxing/EthBlockToNeo4j/addr_set_12597941_12626175')




addr_dict = dict()
for i, elem in enumerate(set1):
    addr_dict[elem] = str(i)

print(addr_dict['0x86889b1CeA13d370E827e099e3778b17d488B3Dd'.lower()])
store_large_obj(addr_dict, '/data/mingxing/EthBlockToNeo4j/addr_dict.p')

with open(f'{os.getcwd()}/address.csv', 'w') as myfile:
    wr = csv.writer(myfile, doublequote=False, quoting=csv.QUOTE_NONE,delimiter=' ', escapechar=' ',skipinitialspace=False)
    for key,value in addr_dict.items():
        wr.writerow([f'{value},{key}'])

print('finish store the csv')