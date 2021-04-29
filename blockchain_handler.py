import os
import json
import time

from blockchain_util import BlockChainUtil
from web3 import Web3

class BlockchainHandler():
    def __init__(self, ws_provider, repository=None):
        self._ws_provider = ws_provider
        self.__contract = None
        self._contract_name = ""
        self._contract_address = "0x0"
        self._initialize_blockchain()

    def _get_base_contract_path(self):
        pass
        return ""

    def _initialize_blockchain(self):
        self._blockchain_util = BlockChainUtil("WS_PROVIDER", self._ws_provider)

    def _get_contract(self, net_id):
        if not self.__contract:
            base_contract_path = self._get_base_contract_path()
            self.__contract = self._blockchain_util.get_contract_instance(
            base_contract_path, self._contract_name, net_id=net_id)
        return self.__contract

    def _call_contract_function(self, method_name, positional_inputs, net_id):
        contract = self._get_contract(net_id)
        return self._blockchain_util.call_contract_function(
            contract=contract, contract_function=method_name, positional_inputs=positional_inputs)

    def _await_transaction(self, transaction_hash):
        while True:
            try:
                thash = self._blockchain_util.get_transaction(transaction_hash)
            except Exception:
                print(f"Waiting for {transaction_hash} to come thru")
                time.sleep(1)
                continue

            if 'blockHash' in thash and thash['blockHash'] is not None:
                print(f"{thash} mined successfully")
                break
            time.sleep(1)

    def _get_events_from_blockchain(self, net_id, start_block_number, end_block_number):
        contract = self._get_contract(net_id)
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

    def _read_contract_events(self, net_id, start_block_number, end_block_number):
        events = self._get_events_from_blockchain(net_id, 
            start_block_number, end_block_number)
        print(f"read no of events {len(events)}")
        return events

    def read_events(self):
        raise Exception("Not implemented read_events")
    
    def get_contract_address_path(self):
        raise Exception("Not implemented get_contract_address")

    def __generate_blockchain_transaction(self, net_id, executor_address, *positional_inputs, method_name):
        contract_instance = self._get_contract(net_id)

        #self.contract = self.load_contract(path=contract_path)
        #self.contract_address = self.read_contract_address(net_id=net_id, path=contract_address_path, key='address')
        #self.contract_instance = self.contract_instance(contract_abi=self.contract, address=self.contract_address)
        transaction_object = self._blockchain_util.create_transaction_object(net_id, contract_instance, method_name, executor_address, *positional_inputs)
        return transaction_object

    def _make_trasaction(self, net_id, executor_address, private_key, *positional_inputs, method_name):
        transaction_object = self.__generate_blockchain_transaction(
                net_id, executor_address, *positional_inputs, method_name=method_name)

        raw_transaction = self._blockchain_util.sign_transaction_with_private_key(
            transaction_object=transaction_object,
            private_key=private_key)
        transaction_hash = self._blockchain_util.process_raw_transaction(raw_transaction=raw_transaction)
        return transaction_hash