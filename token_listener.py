import os
import time

from blockchain_util import BlockChainUtil, ContractType
from repository import Repository
from web3 import Web3
from config import INFURA_URL

class BlockchainEventProcessor():
    def __init__(self, ws_provider, repository=None, ):
        self._blockchain_util = BlockChainUtil("WS_PROVIDER", ws_provider)
        self._repository = Repository()
        self.__contract = None

    def _get_base_contract_path(self):
        pass

    def _get_contract(self, net_id=1):
        if not self.__contract:
            base_contract_path = self._get_base_contract_path()
            self.__contract = self._blockchain_util.get_contract_instance(
            base_contract_path, self._contract_name, net_id=net_id)
        return self.__contract

    def _call_contract_function(self, method_name, positional_inputs):
        contract = self._get_contract()
        return self._blockchain_util.call_contract_function(
            contract=contract, contract_function=method_name, positional_inputs=positional_inputs)

    def _get_events_from_blockchain(self, start_block_number, end_block_number, net_id):
        contract = self._get_contract()
        contract_events = contract.events
        all_blockchain_events = []

        for attributes in contract_events.abi:
            if attributes['type'] == 'event':
                event_name = attributes['name']
                event_object = getattr(contract.events, event_name)
                blockchain_events = event_object.createFilter(fromBlock=start_block_number,
                                                              toBlock=end_block_number).get_all_entries()
                all_blockchain_events.extend(blockchain_events)

        return all_blockchain_events

    def _read_contract_events(self, start_block_number, end_block_number, net_id=1):
        events = self._get_events_from_blockchain(
            start_block_number, end_block_number, net_id)
        print(f"read no of events {len(events)}")
        return events

    def read_events(self):
        pass


class TokenEventProcessor(BlockchainEventProcessor):
    BATCH_SIZE = 50000
    balances_dict = {}

    def __init__(self, ws_provider, repository=None):
        super().__init__(ws_provider, repository)
        self._contract_name = "SingularityNetToken"
        self._insert = 'INSERT INTO token_balances ' + \
           '(address, balance_in_cogs, snapshot_date, row_created_date, row_updated_date) ' + \
           'VALUES (%s, %s, current_timestamp, current_timestamp, current_timestamp) ' + \
           'ON DUPLICATE KEY UPDATE balance_in_cogs = %s, row_updated_date = current_timestamp'
        self._insert_values = []

    def _get_balance(self, address):
        start = time.process_time()
        #address = address.lower()
        balance = self._call_contract_function("balanceOf", [Web3.toChecksumAddress(address)])
        print(f"{(time.process_time() - start)} seconds. Balance of {address} is :: {balance}")
        return balance        

    def __batch_execute(self, values, force=False):
        start = time.process_time()
        number_of_rows = len(self._insert_values)
        if (force and number_of_rows > 0) or number_of_rows >= 50:
            self._repository.bulk_query(self._insert, self._insert_values)
            self._insert_values = []
            print(f"{(time.process_time() - start)} seconds. Inserted {number_of_rows} rows")
        self._insert_values.append(tuple(values))

    def _push_balance(self, address, balance):
        self.__batch_execute([address, balance, balance])        

    def _push_event(self, args):
        from_address = str(args["from"]).lower()
        to_address = str(args["to"]).lower()

        from_address_balance = self._get_balance(from_address)
        self.__batch_execute([from_address, from_address_balance, from_address_balance]) 

        to_address_balance = self._get_balance(to_address)
        self.__batch_execute([to_address, to_address_balance, to_address_balance]) 

    def _get_base_contract_path(self):
        return os.path.abspath(
            os.path.join(os.path.dirname(__file__), 'node_modules', 'singularitynet-token-contracts'))

    def process_events(self, events):
        for event in events:
            if 'event' in event:
                args = event['args']
                if event['event'] == "Transfer":
                    #print("Transfer of " + str(args['value']) + " cogs from " + str(
                    #    args["from"] + " to " + str(args["to"])))
                    self._push_event(args)
                #else:
                #    print("Ignored event " + str(event['event']))
        self.__batch_execute([],True) 

    def read_events(self, from_block_number):
        print(
            f"reading token event from {from_block_number} to {from_block_number+self.BATCH_SIZE}")
        events = self._read_contract_events(
            from_block_number, from_block_number+self.BATCH_SIZE)
        self.process_events(events)
        return events


snapshot_start = time.process_time()
tp = TokenEventProcessor(INFURA_URL)
tp.read_events(12161635)
print(f"{(time.process_time() - snapshot_start)} seconds taken")