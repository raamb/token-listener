import os
import time
import sys, getopt

from blockchain_util import BlockChainUtil, ContractType
from repository import Repository
from blockchain_handler import BlockchainHandler
from web3 import Web3
from config import INFURA_URL

class TokenTransferValidator(BlockchainHandler):
    def __init__(self, ws_provider, net_id):
        super().__init__(ws_provider, net_id)
        self._repository = Repository()
        self._contract_name = "SingularityNetToken"
        self._query = 'select * from token_snapshots'
        self._insert = 'INSERT INTO token_transfer_validation ' + \
           '(wallet_address, snapshot_balance_in_cogs, transfer_balance_in_cogs, row_created, row_updated) ' + \
           'VALUES (%s, %s, %s, current_timestamp, current_timestamp) ' + \
           'ON DUPLICATE KEY UPDATE snapshot_balance_in_cogs = %s, transfer_balance_in_cogs = %s, row_updated = current_timestamp'
        self._insert_values = []        
        #'(wallet_address, balance_in_cogs, block_number, snapshot_date, row_created, row_updated) ' + \

    def _get_balance(self, address):
        start = time.process_time()
        #address = address.lower()
        balance = self._call_contract_function("balanceOf", [Web3.toChecksumAddress(address)])
        print(f"{(time.process_time() - start)} seconds. Balance of {address} is :: {balance}")
        return balance        

    def _get_base_contract_path(self):
        return os.path.abspath(
            os.path.join(os.path.dirname(__file__), 'node_modules', 'singularitynet-token-contracts'))

    def __batch_execute(self, values, force=False):
        start = time.process_time()
        number_of_rows = len(self._insert_values)
        if (force and number_of_rows > 0) or number_of_rows >= 50:
            self._repository.bulk_query(self._insert, self._insert_values)
            self._insert_values = []
            print(f"{(time.process_time() - start)} seconds. Inserted {number_of_rows} rows")
        self._insert_values.append(tuple(values))

    def validate(self):
        records = self._repository.execute(self._query)
        for record in records:
            snapshot_address = record['wallet_address']
            snapshot_balance_in_cogs = record['balance_in_cogs']
            transferred_balance_in_cogs = self._get_balance(snapshot_address)
            if snapshot_balance_in_cogs == transferred_balance_in_cogs:
                print(f"Balance verified for address {snapshot_address}. Balance is {transferred_balance_in_cogs}")
            else:
                print(f"FAILURE - Balance does not match for address {snapshot_address}. Transferred Balance is {transferred_balance_in_cogs}, Snapshot Balance is {snapshot_balance_in_cogs}")
            self.__batch_execute([snapshot_address,snapshot_balance_in_cogs,transferred_balance_in_cogs,snapshot_balance_in_cogs, transferred_balance_in_cogs])
        self.__batch_execute([],True)

def print_usage():
    print("USAGE: token_transfer_validator.py -n <netwoprk_id>")

argv = sys.argv[1:]
if len(argv) < 2:
    print_usage()
    sys.exit()

try:
    snapshot_start = time.process_time()
    opts, args = getopt.getopt(argv,"n:h",["network-id="])
    net_id = 0
    starting_blocknumber = ""
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit()
        elif opt in ("-n", "--network-id"):
            print(arg)
            net_id = int(arg)
            print(f"Processing on network {net_id}")
    
    if net_id == 0:
        print_usage()
        sys.exit()
    tp = TokenTransferValidator(INFURA_URL, net_id)
    tp.validate()
    print(f"{(time.process_time() - snapshot_start)} seconds taken")  
except getopt.GetoptError:
    print_usage()
    sys.exit()
