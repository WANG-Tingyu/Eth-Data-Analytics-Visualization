import requests


class Alchemy_request:
    def __init__(self, transaction_craler, from_block, to_block, token_contract_addresses, request_urls, urlidx):
        self.from_block = from_block
        self.to_block = to_block
        self.token_contract_addresses = list(token_contract_addresses.values())
        self.request_url = request_urls[urlidx]
        self.transaction_craler = transaction_craler
        self.allowToChgBatchSize = True

    def make(self):
        return self.recursiveMake(self.from_block, self.to_block)

    def recursiveMake(self, start, end):
        data = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "alchemy_getAssetTransfers",
            "params": [
                {
                    "fromBlock": "{}".format(self.toHex(start)),
                    "toBlock": "{}".format(self.toHex(end)),
                    "contractAddresses": self.token_contract_addresses,
                    "maxCount": "0x3e8",
                    "category": [
                        "token"
                    ]
                }
            ]
        }
        proxypool_url = 'http://127.0.0.1:5555/random'
        httpProxy = requests.get(proxypool_url).text.strip()
        print(f'get the proxy http://{httpProxy}')
        proxies = {
            'http': f'{httpProxy}',
            'https': f'{httpProxy}'
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
        }
        # r = requests.post(self.request_url, data=str(data).replace('\'', '"'), proxies=proxies, headers=headers)
        r = requests.post(self.request_url, data=str(data).replace('\'', '"'), headers=headers)
        transfers = r.json()['result']['transfers']
        if len(transfers) >= 1000:
            self.transaction_craler.token_blocks_per_call -= 50
            self.allowToChgBatchSize = False
            return self.recursiveMake(start, end // 2) + self.recursiveMake(end // 2 + 1, end)
        elif 1000 - len(transfers) > 300 and self.allowToChgBatchSize:
            self.transaction_craler.token_blocks_per_call += 50
            return transfers
        else:
            return transfers

    def DecimalToHex(self, dec):
        digits = "0123456789ABCDEF"
        x = (dec % 16)
        rest = dec // 16
        if rest == 0:
            return digits[x]
        return self.DecimalToHex(rest) + digits[x]

    def toHex(self, dec):
        return '0x{}'.format(self.DecimalToHex(dec))
