import sys, getopt
import time
import csv
from repository import Repository
from decimal import Decimal
from config import INFURA_URL
from blockchain_util import BlockChainUtil

class Snapshotter():
    def __init__(self,ws_provider):
        self._ws_provider = ws_provider
        self._insert = 'INSERT INTO token_snapshots ' + \
        '(wallet_address, is_contract, balance_in_cogs, block_number, snapshot_date, row_created, row_updated) ' + \
        'VALUES (%s, %s, %s, 0, current_timestamp, current_timestamp, current_timestamp) ' + \
        'ON DUPLICATE KEY UPDATE balance_in_cogs = %s, row_updated = current_timestamp'
        self._repository = Repository()
        self._batch_values = []

    def _batch_execute(self, values, force=False):
        start = time.process_time()
        number_of_rows = len(self._batch_values)
        if (force and number_of_rows > 0) or number_of_rows >= 50:
            self._repository.bulk_query(self._insert, self._batch_values)
            print(f"{(time.process_time() - start)} seconds. Inserted {number_of_rows} rows")
            self._batch_values.clear()
        #print("Batch size is " + str(len(batch_values)))
        self._batch_values.append(tuple(values))

    def process_file(self, holdings):
        with open(holdings, 'rt')as f:
            data = csv.reader(f, delimiter=',')
            blockchain_util = BlockChainUtil("WS_PROVIDER", self._ws_provider)
            for row in data:
                    address = str(row[0]).lower()
                    is_contract = 0
                    if address.startswith("0x"):
                        try:
                            contract_code = blockchain_util.get_code(address)
                        except Exception:
                            print("Renewing websocket connection")
                            blockchain_util = BlockChainUtil("WS_PROVIDER", self._ws_provider)
                            contract_code = blockchain_util.get_code(address)

                        if len(contract_code) > 3:
                            print(f"Found contract {address}")
                            is_contract = 1
                        balance_in_cogs = Decimal(row[1]) * Decimal(100000000)
                        self._batch_execute([address,is_contract, balance_in_cogs,balance_in_cogs])
                    else:
                        print("Ignoring " + address) 

def print_usage():
    print("USAGE: token_snapshot.py -i <token_holdings_csv_file>")

argv = sys.argv[1:]
if len(argv) < 1:
    print_usage()
    sys.exit()

try:
    snapshot_start = time.process_time()
    opts, args = getopt.getopt(argv,"h:i:",["input-file="])
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit()
        elif opt in ("-i", "--input-file"):
            print("Processing file "+str(arg))
            s = Snapshotter(INFURA_URL)
            s.process_file(str(arg))
            print(f"{(time.process_time() - snapshot_start)} seconds taken") 
except getopt.GetoptError:
    print_usage()
    sys.exit()
