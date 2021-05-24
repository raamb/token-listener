import sys, getopt, os
import time
from web3 import Web3

from repository import Repository
from config import INFURA_URL, TOTAL_COGS_TO_TRANSFER, TRANSFERER_PRIVATE_KEY, TRANSFERER_ADDRESS, TOTAL_COGS_TO_APPROVE
from blockchain_handler import BlockchainHandler

COMMON_CNTRCT_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), 'node_modules', 'singularitynet-platform-contracts'))

     
#TODO POST DEPLOYMENT
class TokenTransfer(BlockchainHandler):
    def __init__(self, ws_provider, net_id, dry_run, limit_of_transfers):
        super().__init__(ws_provider, net_id)
        self._contract_name = 'TokenBatchTransfer'
        self._limit_of_transfers = limit_of_transfers
        offset = 100
        if self._limit_of_transfers < 100:
            offset = self._limit_of_transfers
        self._query = 'SELECT * from token_snapshots where balance_in_cogs > 0 and is_contract = 0 and wallet_address not in '  + \
                       '(SELECT wallet_address from transfer_info where transfer_status = \'SUCCESS\')  order by balance_in_cogs desc LIMIT {offset}'.format(offset=offset)
        self._insert = 'INSERT INTO transfer_info ' + \
        '(wallet_address, transfer_fees, transfer_time, transfer_transaction, transfer_status, transfer_amount_in_cogs, row_created, row_updated) ' + \
        'VALUES (%s, 0, current_timestamp, %s, %s, %s, current_timestamp, current_timestamp) '
        # 'ON DUPLICATE KEY UPDATE balance_in_cogs = %s, row_updated = current_timestamp'
        self._repository = Repository()
        self._dry_run = dry_run
        self._approve = False
        self._deposit = False
        self._balances = dict()
        self._batchsize = 100
        self._offset = 0

    def _get_base_contract_path(self):
        return os.path.abspath(
            os.path.join(os.path.dirname(__file__), 'node_modules', 'batch-token-transfer'))
    
    def _get_contract_address(self):
        contract_network_path, contract_abi_path = self._blockchain_util. get_contract_file_paths(self._get_base_contract_path(), self._contract_name)
        self._contract_address = self._blockchain_util.read_contract_address(net_id=self._net_id, path=contract_network_path, key='address')
        return self._contract_address

    def _insert_transaction(self, transaction_hash, status):
        start = time.process_time()
        transaction_data = []
        for address in self._balances:
            transaction_data.append([address,transaction_hash,status,str(self._balances[address])])
        if not self._dry_run:
            self._repository.bulk_query(self._insert, transaction_data)
        print(f"{(time.process_time() - start)} seconds taken to insert transaction")

    #def _approve_deposit_funds(self):
    #    address = self._get_contract_address()
    #    if self._approve:
    #        print("Approving transfer for address " + str(address))
    #        if not self._dry_run:
    #            self._agi_handler.approve_transfer(address,TOTAL_COGS_TO_APPROVE)
    #    if self._deposit:
    #        print("Transfer for address " + str(address))
    #        if not self._dry_run:
    #            self._agi_handler.deposit(address,TOTAL_COGS_TO_TRANSFER)

    def _transfer_tokens_impl(self, *positional_inputs):
        transaction_hash = None
        try:
            transaction_hash = self._make_trasaction(TRANSFERER_ADDRESS, TRANSFERER_PRIVATE_KEY, *positional_inputs, method_name="batchTransfer")
            print(f"transaction hash {transaction_hash} generated for batchTransfer")
            self._await_transaction(transaction_hash)
        except Exception as e:
            error_message = str(e)
            print(f"ERROR {error_message}")
            if(transaction_hash is None and 'nonce too low' in error_message):
                print("Nonce error - retrying")
                self._initialize_blockchain()
                transaction_hash = self._transfer_tokens_impl(*positional_inputs)
            else:
                status = 'FAILED'
                if transaction_hash is not None:
                    status = 'SUBMITTED'
                else:
                    transaction_hash = 'NOTRANSACTION'
                self._insert_transaction(transaction_hash, status)
                raise e
        return transaction_hash
    
    def _transfer_tokens(self):
        addresses = []
        amounts = []
        for address in self._balances:
            addresses.append(Web3.toChecksumAddress(address))
            amounts.append(self._balances[address])
            if len(amounts) >= self._limit_of_transfers:
                break
        
        positional_inputs = (addresses, amounts)
        if not self._dry_run:
            transaction_hash = self._transfer_tokens_impl(*positional_inputs)
        else:
            transaction_hash = 'dry_run'
            print(f"BATCH TRASNFER {positional_inputs}")
        return transaction_hash   

    def _transfer(self):
        token_holders = self._repository.execute(self._query)
        print(f"Processing {len(token_holders)} records. Total so far {self._offset}, limit is {self._limit_of_transfers}")
        for holder in token_holders:
            address = holder['wallet_address']
            balance_in_cogs = holder['balance_in_cogs']
            self._balances[address] = balance_in_cogs
            print(f"Transferring {balance_in_cogs} cogs to {address}")
        
        if(len(self._balances) == 0):
            print("Completed all transfers")
            return

        print(f"Processing {len(self._balances)} transfers")
        transaction_hash = self._transfer_tokens()
        self._insert_transaction(transaction_hash,'SUCCESS')
        self._offset += len(self._balances)
        self._balances.clear()
       
        if self._offset >= self._limit_of_transfers:
            print(f"Completed allowed transfers {self._limit_of_transfers}")
            return
        
        self._transfer()

    
    def process_transfer(self):
        print("Transferring for network " + str(self._net_id))
        #self._approve_deposit_funds()
        self._transfer()


def print_usage():
    print("USAGE: token_transfer.py -n <network_id> [-l <-1|number_of_transfers> -d]")

argv = sys.argv[1:]
if len(argv) < 2:
    print_usage()
    sys.exit()

try:
    snapshot_start = time.process_time()
    opts, args = getopt.getopt(argv,"n:d:l:h",["input-file="])
    net_id = 3
    approve=False
    deposit=False
    dry_run=False
    limit_of_transfers = 5
    for opt, arg in opts:
        print(opt)
        if opt == '-h':
            print_usage()
            sys.exit()
        elif opt in ("-n", "--network_id"):
            net_id = int(arg)
        elif opt in ("-l","--limit_of_transfers"):
            limit_of_transfers = int(arg)
        #elif opt in ("-a", "--approve"):
        #    print("Approve")
        #   approve = False
        elif opt in ("-d", "--dry_run"):
            print("DRY RUN MODE")
            dry_run = True
    
    if limit_of_transfers == -1:
        limit_of_transfers = 100000000000
    print(f"Transfering tokens with a limit of {limit_of_transfers}")
    t = TokenTransfer(INFURA_URL,net_id,dry_run, limit_of_transfers)
    t.process_transfer()
    print(f"{(time.process_time() - snapshot_start)} seconds taken") 
except getopt.GetoptError:
    print_usage()
    sys.exit()
