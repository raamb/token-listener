import sys, getopt
import time
import csv
from repository import Repository
from decimal import Decimal

global insert
global repository
global batch_values

insert = 'INSERT INTO token_balances ' + \
        '(address, balance_in_cogs, snapshot_date, row_created_date, row_updated_date) ' + \
        'VALUES (%s, %s, current_timestamp, current_timestamp, current_timestamp) ' + \
        'ON DUPLICATE KEY UPDATE balance_in_cogs = %s, row_updated_date = current_timestamp'
repository = Repository()
batch_values = []

def batch_execute(values, force=False):
    start = time.process_time()
    number_of_rows = len(batch_values)
    if (force and number_of_rows > 0) or number_of_rows >= 50:
        repository.bulk_query(insert, batch_values)
        print(f"{(time.process_time() - start)} seconds. Inserted {number_of_rows} rows")
        batch_values.clear()
    #print("Batch size is " + str(len(batch_values)))
    batch_values.append(tuple(values))

def process_file(holdings):
    with open(holdings, 'rt')as f:
        data = csv.reader(f, delimiter=',')
        for row in data:
                address = str(row[0]).lower()
                if address.startswith("0x"):
                    balance_in_cogs = Decimal(row[1]) * Decimal(100000000)
                    batch_execute([address,balance_in_cogs,balance_in_cogs])
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
            process_file(str(arg))
            print(f"{(time.process_time() - snapshot_start)} seconds taken") 
except getopt.GetoptError:
    print_usage()
    sys.exit()
