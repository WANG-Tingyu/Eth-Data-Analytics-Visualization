import os
import csv
import pickle

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
    

def tx_list2csv(node_dict, filePath, csvname):
    tx_list = load_large_obj(filePath)
    for elem in tx_list:
        elem['from_id'] = node_dict[elem['from']]
        elem['to_id'] = node_dict[elem['to']]

    header = ['from_id', 'blockNumber', 'hash', 'value', 'from', 'to', 'asset', 'timestamp', 'to_id']
    with open(f'{os.getcwd()}/{csvname}', 'wt') as f:
        csv_writer = csv.DictWriter(f, fieldnames=header)
        csv_writer.writerows(tx_list)
    print(f'{csvname} finish ')

node_dict = load_large_obj(f'{os.getcwd()}/addr_dict.p')
# tx_list2csv(node_dict,f'{os.getcwd()}/tx_list_1_1000000', 'tx_list_1_1000000.csv')
# tx_list2csv(node_dict,f'{os.getcwd()}/tx_list_1000001_2000000', 'tx_list_1000001_2000000.csv')
# tx_list2csv(node_dict,f'{os.getcwd()}/tx_list_2000001_3000000', 'tx_list_2000001_3000000.csv')
# tx_list2csv(node_dict,f'{os.getcwd()}/tx_list_3000001_4000000', 'tx_list_3000001_4000000.csv')
# tx_list2csv(node_dict,f'{os.getcwd()}/tx_list_4000001_5000000', 'tx_list_4000001_5000000.csv')
# tx_list2csv(node_dict,f'{os.getcwd()}/tx_list_5000001_6000000', 'tx_list_5000001_6000000.csv')
# tx_list2csv(node_dict,f'{os.getcwd()}/tx_list_6000001_7000000', 'tx_list_6000001_7000000.csv')
# tx_list2csv(node_dict,f'{os.getcwd()}/tx_list_7000001_7500000', 'tx_list_7000001_7500000.csv')
# tx_list2csv(node_dict,f'{os.getcwd()}/tx_list_7500001_8000000', 'tx_list_7500001_8000000.csv')
# tx_list2csv(node_dict,f'{os.getcwd()}/tx_list_8000001_8500000', 'tx_list_8000001_8500000.csv')
# tx_list2csv(node_dict,f'{os.getcwd()}/tx_list_8500001_9000000', 'tx_list_8500001_9000000.csv')
# tx_list2csv(node_dict,f'{os.getcwd()}/tx_list_9000001_9500000', 'tx_list_9000001_9500000.csv')
# tx_list2csv(node_dict,f'{os.getcwd()}/tx_list_9500001_10000000', 'tx_list_9500001_10000000.csv')
# tx_list2csv(node_dict,f'{os.getcwd()}/tx_list_10000001_10500000', 'tx_list_10000001_10500000.csv')
# tx_list2csv(node_dict,f'{os.getcwd()}/tx_list_10500001_11000000', 'tx_list_10500001_11000000.csv')
# tx_list2csv(node_dict,f'{os.getcwd()}/tx_list_11000001_11500000', 'tx_list_11000001_11500000.csv')
# tx_list2csv(node_dict,f'{os.getcwd()}/tx_list_11500001_12000000', 'tx_list_11500001_12000000.csv')
# tx_list2csv(node_dict,f'{os.getcwd()}/tx_list_12000001_12500000', 'tx_list_12000001_12500000.csv')
# tx_list2csv(node_dict,f'{os.getcwd()}/tx_list_12500001_12595900', 'tx_list_12500001_12595900.csv')
# tx_list2csv(node_dict,f'{os.getcwd()}/tx_list_12595901_12597940', 'tx_list_12595901_12597940.csv')
tx_list2csv(node_dict,f'{os.getcwd()}/tx_list_12597941_12626175', 'tx_list_12597941_12626175.csv')



