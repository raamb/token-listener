import sys, getopt
import time
import csv
from repository import Repository
from decimal import Decimal
from config import INFURA_URL
from agi_token_handler import AGITokenHandler

class Snapshotter():
    def __init__(self,ws_provider,net_id):
        self._ws_provider= ws_provider
        self._net_id = net_id
        self._query = 'select wallet_address, is_contract from token_snapshots'
        self._insert = 'INSERT INTO token_snapshots ' + \
        '(wallet_address, is_contract, balance_in_cogs, block_number, snapshot_date, row_created, row_updated) ' + \
        'VALUES (%s, %s, %s, 0, current_timestamp, current_timestamp, current_timestamp) ' + \
        'ON DUPLICATE KEY UPDATE balance_in_cogs = %s, row_updated = current_timestamp'
        self._repository = Repository()
        self._batch_values = []
        self._agi_token_handler = AGITokenHandler(self._ws_provider,self._net_id,False)

    def _batch_execute(self, values, force=False):
        start = time.process_time()
        number_of_rows = len(self._batch_values)
        if (force and number_of_rows > 0) or number_of_rows >= 50:
            self._repository.bulk_query(self._insert, self._batch_values)
            print(f"{(time.process_time() - start)} seconds. Inserted {number_of_rows} rows")
            self._batch_values.clear()
        self._batch_values.append(tuple(values))

    def dump_balances(self):
        balances = open('AGITokenBalances_' + time.strftime("%Y%m%d-%H%M%S") + '.csv', mode='w')
        balances_file = csv.writer(balances, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        balances_file.writerow(["Address", "Token Balance", "Is Contract"])
        records = self._repository.execute(self._query)
        for record in records:
            wallet_address = record['wallet_address']
            is_contract = (record['is_contract']==b"\x01")
            balance_in_cogs = self._agi_token_handler._get_balance(wallet_address)
            balances_file.writerow([wallet_address, balance_in_cogs, is_contract])

    def process_file(self, holdings):
        with open(holdings, 'rt')as f:
            data = csv.reader(f, delimiter=',')
            for row in data:
                    address = str(row[0]).lower()
                    is_contract = 0
                    if address.startswith("0x"):
                        snapshot_balance_in_cogs = Decimal(row[1]) * 100000000
                        try:
                            contract_code = self._agi_token_handler.get_code(address)
                        except Exception:
                            print("Renewing websocket connection")
                            self._agi_token_handler = AGITokenHandler(self._ws_provider,self._net_id, False)
                            contract_code = self._agi_token_handler.get_code(address)

                        if len(contract_code) > 3:
                            print(f"Found contract {address}")
                            is_contract = 1
                        balance_in_cogs = self._agi_token_handler._get_balance(address)
                        if balance_in_cogs != snapshot_balance_in_cogs:
                            print(f"BALANCE MISMATCH for {address} Blockchain - {balance_in_cogs} SNAPSHOT - {snapshot_balance_in_cogs}")
                        self._batch_execute([address,is_contract, balance_in_cogs,balance_in_cogs])
                    else:
                        print("Ignoring " + address)
            self._batch_execute([],True)
            print("BATCH COMPLETED")

def print_usage():
    print("USAGE: agi_token_snapshot.py [-d | -i <token_holdings_csv_file>] -n <net_id>")

argv = sys.argv[1:]
try:
    snapshot_start = time.process_time()
    opts, args = getopt.getopt(argv,"dh:n:i:",["net-id=","input-file=","dump-balances="])
    net_id = 0
    in_file = None
    dump_balances = False
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit()
        elif opt in ("-d", "--dump-balances"):
            print("Dump balances")
            dump_balances = True
        elif opt in ("-i", "--input-file"):
            in_file = str(arg)
            print("Processing file "+ in_file)
        elif opt in ("-n", "--network-id"):
            net_id = int(arg)
            print(f"Processing on network {net_id}")
    
    if net_id == 0:
        print_usage()
        sys.exit()
    
    if (dump_balances and in_file is not None) or (dump_balances == False and in_file is None):
        print_usage()
        sys.exit()

    s = Snapshotter(INFURA_URL, net_id)
    if dump_balances:
        s.dump_balances()
    else:
        s.process_file(in_file)
    print(f"{(time.process_time() - snapshot_start)} seconds taken") 
except getopt.GetoptError:
    print_usage()
    sys.exit()
