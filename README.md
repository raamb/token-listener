## Token Migration utilities
A bunch of utility python scripts to check token balances, transfer tokens and validate transfers.
The code can be made a lot more efficient but is functional for now as its essentially for one time use.
The schema needed for this to work is in the schema.sql file

## Running
* Ensure that you have Python 3.9 or above
* Run > pip install -r requirements.txt
* Run > npm i singularitynet-token-contracts
* You are now ready to run the program

## Utilities
### token_snapshot.py
When run with the -d option it dumps the current AGI token balances for each address in the token_snapshots table into a csv file
When run with the -i option reads the file provided and populates token_snapshots table. Balance for each address is obtained from blockchain

### agi_token_listener.py
Starts listening for transfer events from the give block number and updates it in the token_snapshots table

### token_transfer.py
Transfers AGIX tokens based on token_snapshots to corresponding addresses using the batch token transfer contract. Transfer details are stored in the transfer_info table

### token_transfer_validator.py
Validates the AGIX token balance on blockchain against the AGI token balance in token_snapshots
