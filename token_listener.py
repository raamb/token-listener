import os
import time
import sys, getopt
import csv

from blockchain_util import BlockChainUtil, ContractType
from repository import Repository
from web3 import Web3
from config import INFURA_URL
from agi_token_handler import AGITokenHandler

class TokenEventProcessor(AGITokenHandler):
    BATCH_SIZE = 100
    balances_dict = {}

    def __init__(self, ws_provider, net_id,is_agix, validate_transfers):
        super().__init__(ws_provider, net_id,is_agix)
        self._insert_snapshot = 'INSERT INTO token_snapshots ' + \
           '(wallet_address, balance_in_cogs, block_number, snapshot_date, row_created, row_updated) ' + \
           'VALUES (%s, %s, %s, current_timestamp, current_timestamp, current_timestamp) ' + \
           'ON DUPLICATE KEY UPDATE balance_in_cogs = %s, block_number = %s, row_updated = current_timestamp'
        self._insert_values = []
        self._validate_transfers = validate_transfers

        self._query = 'select wallet_address, balance_in_cogs from token_snapshots where wallet_address in (%s)'
        self._insert_validate = 'INSERT INTO token_transfer_validation ' + \
           '(wallet_address, is_contract, snapshot_balance_in_cogs, transfer_balance_in_cogs, row_created, row_updated) ' + \
           'VALUES (%s, %s, %s, %s, current_timestamp, current_timestamp) ' + \
           'ON DUPLICATE KEY UPDATE row_updated = current_timestamp'
        self._transfer_amounts = {}

    def __batch_execute(self, values, force=False):    
        start = time.process_time()
        sql_stmt = self._insert_snapshot
        if self._validate_transfers:
            sql_stmt = self._insert_validate          

        number_of_rows = len(self._insert_values)
        if (force and number_of_rows > 0) or number_of_rows >= 50:
            self._repository.bulk_query(sql_stmt, self._insert_values)
            self._insert_values = []
            print(f"{(time.process_time() - start)} seconds. Inserted {number_of_rows} rows")
        if len(values) > 0:
            self._insert_values.append(tuple(values))

    def _validate_and_update(self, block_number, from_address, to_address, to_address_balance, force):
        if to_address is not None:
            print(f"TRANSFER {from_address} to {to_address} of {to_address_balance} tokens")
            if to_address in self._transfer_amounts:
                to_address_balance += self._transfer_amounts[to_address]
            self._transfer_amounts[to_address] = to_address_balance
        
        if (len(self._transfer_amounts) > 0 and force) or len(self._transfer_amounts) >= 50:
            list_of_wallets = self._transfer_amounts.keys()
            format_strings = ','.join(['%s'] * len(list_of_wallets))
            select_query = self._query % format_strings
            print(f"SELECT {select_query}")
            records = self._repository.execute(select_query,tuple(list_of_wallets))
            for record in records:
                snapshot_address = record['wallet_address']
                snapshot_balance_in_cogs = record['balance_in_cogs']
                to_address_balance = self._transfer_amounts[snapshot_address]
                
                is_contract = 0
                contract_code = self._blockchain_util.get_code(snapshot_address)
                if len(contract_code) > 3:
                    print(f"Found contract {snapshot_address}")
                    is_contract = 1

                if snapshot_balance_in_cogs == to_address_balance:
                    print(f"Balance verified for address {snapshot_address}. Balance is {to_address_balance}")
                else:
                    print(f"FAILURE - Balance does not match for address {snapshot_address}. Transferred Balance is {to_address_balance}, Snapshot Balance is {snapshot_balance_in_cogs}")
                self.__batch_execute([snapshot_address,is_contract, snapshot_balance_in_cogs,to_address_balance],force)
                
            self._transfer_amounts.clear()

    def _push_event(self, block_number, from_address, to_address):
        from_address_balance = self._get_balance(from_address)
        self.__batch_execute([from_address, from_address_balance, block_number, from_address_balance, block_number]) 

        to_address_balance = self._get_balance(to_address)
        self.__batch_execute([to_address, to_address_balance, block_number, to_address_balance, block_number]) 
        
    def _update_balances(self, block_number, from_address, to_address):
        from_address_balance = self._get_balance(from_address)
        self.__batch_execute([from_address, from_address_balance, block_number, from_address_balance, block_number]) 

        to_address_balance = self._get_balance(to_address)
        self.__batch_execute([to_address, to_address_balance, block_number, to_address_balance, block_number])               
        self._push_event(block_number, from_address, to_address)
        self.__batch_execute([],False)

    def process_events(self, events):
        for event in events:
            if 'event' in event:
                event_args = event['args']
                #print("Transfer of " + str(args['value']) + " cogs from " + str(
                #    args["from"] + " to " + str(args["to"])))
                print(event_args)
                from_address = str(event_args["from"]).lower()
                to_address = str(event_args["to"]).lower()
                value = event_args["value"]
                block_number = event['blockNumber']

                if self._validate_transfers:
                    self._validate_and_update(block_number, from_address, to_address, value, False)
                else:                   
                    self._update_balances(block_number, from_address, to_address)

    def read_events(self, from_block_number, from_address):
        end_block_number = self._blockchain_util.get_current_block_no()
        while True:
            to_block_number = from_block_number+int(self.BATCH_SIZE)
            if to_block_number > end_block_number:
                print(f"Done with all events till {end_block_number}")
                break

            print(f"reading token event from {from_block_number} to {to_block_number}")
            events = self._read_contract_events(
                from_block_number, to_block_number, 'Transfer', from_address)
            self.process_events(events)
            from_block_number = to_block_number+1
        if self._validate_transfers:
            self._validate_and_update(None,None,None,None,True)
        else:
            self.__batch_execute([],True) 
        return events

def print_usage():
    print("USAGE: agi_token_listener.py -s <starting-blocknumber> -n <netwoprk_id> [-v -f <from_address>]")

argv = sys.argv[1:]
#print(argv)
if len(argv) < 4:
    print_usage()
    sys.exit()

try:
    snapshot_start = time.process_time()
    opts, args = getopt.getopt(argv,"s:n:f:vh",["starting-blocknumber=","network-id="])
    #print(opts)
    net_id = 0
    starting_blocknumber = None
    from_address = None
    validate_transfers = False
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit()
        elif opt in ("-v", "--validate-transfers"):
            validate_transfers = True
            print(f"Validating transfers")
        elif opt in ("-f", "--from-address"):
            value = str(arg).lower()
            print(f"Reading events from {value}")
            from_address = Web3.toChecksumAddress(value)
            print(f"Reading events from {from_address}")
        elif opt in ("-s", "--starting-blocknumber"):
            starting_blocknumber = int(arg)
            print(f"Processing from {starting_blocknumber}")
        elif opt in ("-n", "--network-id"):
            print(arg)
            net_id = int(arg)
            print(f"Processing on network {net_id}")
    
    if starting_blocknumber is None or net_id == 0 or (validate_transfers and from_address is None):
        print_usage()
        sys.exit()

    tp = TokenEventProcessor(INFURA_URL,net_id, validate_transfers, validate_transfers)
    tp.read_events(starting_blocknumber, from_address)
    print(f"{(time.process_time() - snapshot_start)} seconds taken")  
except getopt.GetoptError:
    print_usage()
    sys.exit()
