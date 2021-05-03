import os
import time
import sys, getopt
import csv

from blockchain_util import BlockChainUtil, ContractType
from repository import Repository
from web3 import Web3
from config import INFURA_URL
from agi_token_handler import AGITokenHandler

class TokenBalanceWriter(AGITokenHandler):
    def __init__(self, ws_provider, net_id, repository=None):
        super().__init__(ws_provider, net_id)
        self._query = 'select * from token_holders';
        balances = open('AGITokenBalances.csv', mode='w')
        self.balances_file = csv.writer(balances, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        self.balances_file.writerow(["Address", "Token Balance", "Is Contract"])

    def dump_balances(self):
        records = self._repository.execute(self._query)
        for record in records:
            wallet_address = record['wallet_address']
            balance_in_cogs = self._get_balance(wallet_address)
            self.balances_file.writerow(wallet_address, balance_in_cogs, record['is_contract'])
 
def print_usage():
    print("USAGE: agi_token_balances.py -n <netwoprk_id>")

argv = sys.argv[1:]
#print(argv)
if len(argv) < 2:
    print_usage()
    sys.exit()

try:
    snapshot_start = time.process_time()
    opts, args = getopt.getopt(argv,"n:h",["network-id="])
    #print(opts)
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
    tp = TokenBalanceWriter(INFURA_URL, net_id)
    tp.read_events(net_id, starting_blocknumber)
    print(f"{(time.process_time() - snapshot_start)} seconds taken")  
except getopt.GetoptError:
    print_usage()
    sys.exit()
