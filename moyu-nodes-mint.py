import requests
import hashlib
import random
from web3 import Web3
import logging
import time
import os
from concurrent.futures import ThreadPoolExecutor,as_completed
from eth_account import Account
import decimal
import sys

logging.basicConfig(
    level=logging.INFO,  
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    handlers=[
        logging.StreamHandler(sys.stdout)  
    ]
)

_logger = logging.getLogger(__name__)
provider = Web3.HTTPProvider(os.getenv("INFRUA_URL", "https://rpc.ankr.com/eth"))
web3 = Web3(provider)

TX_NUM_ONE_BATCH = 10 # default tx number of one batch

class RandomNumberGenerator:
    def __init__(self, start, end):
        self.numbers = list(range(start, end + 1))

    def get_random_number(self):
        if not self.numbers:
            return None  # 所有数字都已经被选择完毕
        index = random.randint(0, len(self.numbers) - 1)
        random_number = self.numbers.pop(index)
        return random_number


def get_unused_tick(data):
    # 将data转为hash
    hash = hashlib.sha256(data.encode("utf-8")).hexdigest()
    # 构建请求URL
    url = f"https://ethscriber.xyz/api/ethscriptions/exists/{hash}"
    # 发送 GET 请求, 获取是否存在
    response = requests.get(url)
    if response.status_code == 200:
        result = response.json()
        if result["result"]:
            _logger.info(f"the {data} has been minted, skip")
            return ""
        _logger.info(f"the {data} is not minted, can be minted")
        hex_representation = hex(int.from_bytes(data.encode(), "big"))
        return hex_representation
    _logger.info(f"Failed to get messages. Status code: {response.status_code}")
    return ""

def _get_best_gas_price():
    gas_price = int(web3.eth.gas_price)
    max_gas_price  = web3.to_wei(os.getenv("GAS_PRICE", 20), "gwei")
    _logger.info(f"now gas price {gas_price * 1.2} max gas price {max_gas_price}")
    if int(gas_price * 1.2) < max_gas_price:
        return int(gas_price * 1.2), gas_price
    return int((max_gas_price - gas_price) * 0.2 + gas_price) , gas_price

def mint_tick(private_key, hex_list):
    address, balance = _get_balance(pk)
    if balance  < decimal.Decimal(os.getenv("MIN_BALANCE", "0")):
        _logger.info(f"address {address} balance {balance} is too low")
        return 
    if not hex_list:
        _logger.info(f"address {address} no ticks")
        return 
    _logger.info(f"start for {address} and ticks {hex_list}")
    amount_in_wei = web3.to_wei(0, "ether")
    gas_price, now_gas_price = _get_best_gas_price()
    _logger.info(f"account {address} mint token,  gas_price={gas_price} now gas price {now_gas_price}")
    nonce = web3.eth.get_transaction_count(address, "pending")
    _logger.info(f"now nonce {nonce}")
    transactions = []
    for data in hex_list:
        transaction = {
            "to": address,
            "gas": 31000,
            "gasPrice": gas_price,
            "nonce": nonce,
            "value": amount_in_wei,
            "chainId": 1,
            "data": data,
        }
        transactions.append(transaction)
        nonce = nonce + 1

    signed_transactions = [
        web3.eth.account.sign_transaction(tx, private_key) for tx in transactions
    ]
    transaction_hashes = [
        web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        for signed_tx in signed_transactions
    ]
    last_tx_hash = ""
    for i, tx_hash in enumerate(transaction_hashes):
        _logger.info(f"Transaction {i + 1} sent. Transaction hash: {tx_hash.hex()}")
        last_tx_hash = tx_hash
    _logger.info(f"tx hash: {last_tx_hash.hex()}")
    web3.eth.wait_for_transaction_receipt(last_tx_hash, timeout=1800)
    return last_tx_hash.hex()


def _get_mint_data_list(start, end, number):
    # 自行替换需要打的data
    # 注意，换成了需要打的data之后，需要将 id: 后面的值 改为 "{}"
    data_template = (
        'data:,{{"p":"erc-20","op":"mint","tick":"nodes","id":"{}","amt":"10000"}}'
    )
    id_generator = RandomNumberGenerator(start, end)
    hex_list = []
    for x in range(number):
        for _ in range(start, end):
            num = id_generator.get_random_number()
            data = data_template.format(num)
            hex = get_unused_tick(data)
            if hex:
                hex_list.append(hex)
                break
    _logger.info(f"now tx id {hex_list}")
    return hex_list

def _get_balance(pk):
    address = Account.from_key(pk).address
    return address, web3.from_wei(web3.eth.get_balance(address), "ether")

if __name__ == "__main__":
    private_key_list = os.getenv("PK", "").split(",")
    if len(private_key_list) == 0:
        _logger.warning("privates is empty")
        exit(1)
    num = len(private_key_list)
    while True:
        gas_price = web3.eth.gas_price / 10**9
        _logger.info(f"now gas={gas_price}")
        if gas_price > int(os.getenv("GAS_PRICE", 100)):
            _logger.info(f"gas={gas_price} is too high")
            time.sleep(5)
            continue
        ticks = _get_mint_data_list(1, 999999, int(os.getenv("PARALLEL_TX_NUM",TX_NUM_ONE_BATCH)) * num)
        if not ticks:
            _logger.info("no ticks")
            time.sleep(5)
            continue
        avg = len(ticks) // num
        tick_list = [ticks[i:i + avg] for i in range(0, len(ticks), avg)]
        with ThreadPoolExecutor(max_workers=num) as executor:
            futures = []
            for i,  pk in enumerate(private_key_list):
                executor.submit(mint_tick, pk, tick_list[i])
            for future in as_completed(futures):
                _logger.info("finished ", future.result())