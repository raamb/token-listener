import os
import time
import sys, getopt

from blockchain_util import BlockChainUtil, ContractType
from repository import Repository
from blockchain_handler import BlockchainHandler
from web3 import Web3
from config import INFURA_URL

class TokenEventProcessor(BlockchainHandler):
    BATCH_SIZE = 200
    balances_dict = {}

    def __init__(self, ws_provider, repository=None):
        super().__init__(ws_provider)
        self._repository = Repository()
        self._contract_name = "SingularityNetToken"
        self._insert = 'INSERT INTO token_holders ' + \
           '(wallet_address, block_number, row_created, row_updated) ' + \
           'VALUES (%s, %s, current_timestamp, current_timestamp) ' + \
           'ON DUPLICATE KEY UPDATE block_number = %s, row_updated = current_timestamp'
        self._insert_transfer = 'INSERT INTO agi_token_transfers ' + \
           '(from_address, to_address, amount_in_cogs, block_number, row_created, row_updated) ' + \
           'VALUES (%s, %s, %s, %s, current_timestamp, current_timestamp) '   
        self._insert_values = []
        self._insert_transfer_values = []

    def _get_balance(self, net_id, address):
        start = time.process_time()
        #address = address.lower()
        balance = self._call_contract_function("balanceOf", [Web3.toChecksumAddress(address)], net_id)
        print(f"{(time.process_time() - start)} seconds. Balance of {address} is :: {balance}")
        return balance        

    def __transfer_batch_execute(self, values, force=False):
        if force and len(self._insert_transfer_values) == 0:
            return

        start = time.process_time()
        number_of_rows = len(self._insert_transfer_values)
        if (force and number_of_rows > 0) or number_of_rows >= 50:
            #print(self._insert_transfer_values)
            self._repository.bulk_query(self._insert_transfer, self._insert_transfer_values)
            self._insert_transfer_values = []
            print(f"Transfer {(time.process_time() - start)} seconds. Inserted {number_of_rows} rows")
        self._insert_transfer_values.append(tuple(values))

    def __batch_execute(self, values, force=False):
        if force and len(self._insert_values) == 0:
            return

        start = time.process_time()
        number_of_rows = len(self._insert_values)
        if (force and number_of_rows > 0) or number_of_rows >= 50:
            #print(self._insert_values)
            self._repository.bulk_query(self._insert, self._insert_values)
            self._insert_values = []
            print(f"{(time.process_time() - start)} seconds. Inserted {number_of_rows} rows")
        self._insert_values.append(tuple(values))

  
    def _push_event(self, net_id, block_number, args):
        #print(args)
        from_address = str(args["from"]).lower()
        to_address = str(args["to"]).lower()
        amount = args["value"]

        self.__batch_execute([from_address, block_number,block_number]) 
        self.__batch_execute([to_address, block_number,block_number]) 
        self.__transfer_batch_execute([from_address,to_address,amount,block_number])

    def _get_base_contract_path(self):
        return os.path.abspath(
            os.path.join(os.path.dirname(__file__), 'node_modules', 'singularitynet-token-contracts'))

    def process_events(self, net_id, events):
        #print("Processing events")
        for event in events:
            if 'event' in event:
                args = event['args']
                if event['event'] == "Transfer":
                    print("Transfer of " + str(args['value']) + " cogs from " + str(
                        args["from"] + " to " + str(args["to"])))
                    self._push_event(net_id, event['blockNumber'], args)
                else:
                    print("Ignored event " + str(event['event']))

    def read_events(self, net_id, from_block_number):
        end_block_number = 12347421
        while True:
            to_block_number = from_block_number+int(self.BATCH_SIZE)
            if to_block_number > end_block_number:
                print("Done with all events")
                break

            print(
                f"reading token event from {from_block_number} to {to_block_number}")
            try:
                events = self._read_contract_events(net_id,
                    from_block_number, to_block_number)
                self.process_events(net_id, events)
                from_block_number = to_block_number
            except Exception as e:
                print(f"Excetion {e}. Reinitializing Blockchain")
                self._initialize_blockchain()
                continue
            
        self.__batch_execute([],True) 
        self.__transfer_batch_execute([],True) 
        return events

def print_usage():
    print("USAGE: token_listener.py -s <starting-blocknumber> -n <netwoprk_id>")

argv = sys.argv[1:]
#print(argv)
if len(argv) < 4:
    print_usage()
    sys.exit()

try:
    snapshot_start = time.process_time()
    opts, args = getopt.getopt(argv,"s:n:h",["starting-blocknumber=","network-id="])
    #print(opts)
    net_id = 0
    starting_blocknumber = ""
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit()
        elif opt in ("-s", "--starting-blocknumber"):
            starting_blocknumber = int(arg)
            print(f"Processing from {starting_blocknumber}")
        elif opt in ("-n", "--network-id"):
            print(arg)
            net_id = int(arg)
            print(f"Processing on network {net_id}")
    
    if starting_blocknumber == "" or net_id == 0:
        print_usage()
        sys.exit()
    tp = TokenEventProcessor(INFURA_URL)
    tp.read_events(net_id, starting_blocknumber)
    print(f"{(time.process_time() - snapshot_start)} seconds taken")  
except getopt.GetoptError:
    print_usage()
    sys.exit()