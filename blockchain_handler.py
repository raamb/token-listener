import time

from blockchain_util import BlockChainUtil

class BlockchainHandler():
    def __init__(self, ws_provider, net_id, repository=None):
        self._ws_provider = ws_provider
        self._net_id = net_id
        self.__contract = None
        self._contract_name = ""
        self._contract_address = "0x0"
        self._initialize_blockchain()

    def _get_base_contract_path(self):
        pass
        return ""

    def _initialize_blockchain(self):
        self.__contract = None
        if self._ws_provider.startswith('wss://'):
            self._blockchain_util = BlockChainUtil("WS_PROVIDER", self._ws_provider)
        else:
            self._blockchain_util = BlockChainUtil("HTTP_PROVIDER", self._ws_provider)

    def _get_contract(self):
        if not self.__contract:
            base_contract_path = self._get_base_contract_path()
            self.__contract = self._blockchain_util.get_contract_instance(
            base_contract_path, self._contract_name, net_id=self._net_id)
        return self.__contract

    def _call_contract_function(self, method_name, positional_inputs):
        contract = self._get_contract()
        return self._blockchain_util.call_contract_function(
            contract=contract, contract_function=method_name, positional_inputs=positional_inputs)

    def _await_transaction(self, transaction_hash):
        retry_count = 130
        thash = None
        
        while True:
            try:
                thash = self._blockchain_util.get_transaction_receipt_from_blockchain(transaction_hash)
            except Exception:
                print(f"Waiting for {transaction_hash} to come thru")
            
            if thash is None:
                print(f"Waiting for {transaction_hash} to be mined")
                time.sleep(2)
                retry_count -= 1
                if retry_count <= 0:
                    print(f"MINING FAILURE for {transaction_hash}")
                    raise RuntimeError("MINING FAILURE for " + str(transaction_hash))
            else:
               print(f"MINED {thash}")
               break


    def _get_events_from_blockchain(self, start_block_number, end_block_number, event_name, from_address=None):
        contract = self._get_contract()
        event_object = getattr(contract.events, event_name)
        if from_address:
            transfer_events = event_object.createFilter(fromBlock=start_block_number,
                                           toBlock=end_block_number, argument_filters={'from': from_address})
        else:
            transfer_events = event_object.createFilter(fromBlock=start_block_number,
                                           toBlock=end_block_number)
        return transfer_events.get_all_entries()
        #all_blockchain_events = []
        #contract_events = contract.events
        #for attributes in contract_events.abi:
        #    if attributes['type'] == 'event':
        #        event_name = attributes['name']
        #        event_object = getattr(contract.events, event_name)
        #        blockchain_events = event_object.createFilter(fromBlock=start_block_number,
        #                                                      toBlock=end_block_number).get_all_entries()
        #        all_blockchain_events.extend(blockchain_events)
        #return all_blockchain_events

    def _read_contract_events(self, start_block_number, end_block_number, event_name, from_address):
        events = self._get_events_from_blockchain(start_block_number, end_block_number, event_name, from_address)
        print(f"read no of events {len(events)}")
        return events

    def read_events(self):
        raise Exception("Not implemented read_events")
    
    def get_contract_address_path(self):
        raise Exception("Not implemented get_contract_address")

    def __generate_blockchain_transaction(self, executor_address, *positional_inputs, method_name):
        contract_instance = self._get_contract()

        #self.contract = self.load_contract(path=contract_path)
        #self.contract_address = self.read_contract_address(net_id=net_id, path=contract_address_path, key='address')
        #self.contract_instance = self.contract_instance(contract_abi=self.contract, address=self.contract_address)
        transaction_object = self._blockchain_util.create_transaction_object(self._net_id, contract_instance, method_name, executor_address, *positional_inputs)
        return transaction_object

    def _make_trasaction(self, executor_address, private_key, *positional_inputs, method_name):
        transaction_object = self.__generate_blockchain_transaction(
                executor_address, *positional_inputs, method_name=method_name)

        raw_transaction = self._blockchain_util.sign_transaction_with_private_key(
            transaction_object=transaction_object,
            private_key=private_key)
        transaction_hash = self._blockchain_util.process_raw_transaction(raw_transaction=raw_transaction)
        return transaction_hash
