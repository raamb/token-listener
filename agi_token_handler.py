import sys, getopt, os
import time
import csv
from web3 import Web3

from repository import Repository
from decimal import Decimal
from config import INFURA_URL, TOTAL_COGS_TO_TRANSFER, TRANSFERER_PRIVATE_KEY, TRANSFERER_ADDRESS, TOTAL_COGS_TO_APPROVE
from blockchain_handler import BlockchainHandler

class AGITokenHandler(BlockchainHandler):
    def __init__(self, ws_provider, net_id, repository=None):
        super().__init__(ws_provider,net_id)
        self._repository = Repository()
        self._contract_name = "SingularityNetToken"

    def _get_base_contract_path(self):
        return os.path.abspath(
            os.path.join(os.path.dirname(__file__), 'node_modules', 'agi-singularitynet-token-contracts'))

    def get_code(self, address):
        return self._blockchain_util.get_code(address)

    def _get_balance(self, address):
        start = time.process_time()
        #address = address.lower()
        balance = self._call_contract_function("balanceOf", [Web3.toChecksumAddress(address)])
        print(f"{(time.process_time() - start)} seconds. Balance of {address} is :: {balance}")
        return balance

    def _await_transaction(self, transaction_hash):
        while True:
            thash = self._blockchain_util.get_transaction(transaction_hash)
            #print(f"Receipt is {thash}")
            if 'blockHash' in thash and thash['blockHash'] is not None:
                print(f"{thash} mined successfully")
                break
            time.sleep(1)

    def _invoke(self, method_name, address, amount_in_cogs):
        print(f"Processing transacton for {method_name} to {address} for {amount_in_cogs}")
        positional_inputs = (Web3.toChecksumAddress(address), amount_in_cogs)
        transaction_hash = self._make_trasaction(self._net_id, TRANSFERER_ADDRESS, TRANSFERER_PRIVATE_KEY, *positional_inputs, method_name=method_name)
        print(f"transaction hash {transaction_hash} generated for {method_name} to {address} for {amount_in_cogs}")
        self._await_transaction(transaction_hash)
        return transaction_hash

    def deposit(self, address, amount_in_cogs):
        return self._invoke("transfer", address, amount_in_cogs)

    def approve_transfer(self, address, amount_in_cogs):
        return self._invoke("approve", address, amount_in_cogs)