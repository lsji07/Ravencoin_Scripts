# Tool 001 - Rewards Script

# This script is to help people generally easily issue reward assets to their asset holders on Ravencoin.

# Version: 1.0.4
# Initial Author: LSJI07
# Contributor: cereberuscx

# The script takes some user input and setup and in intended to help you get familiar with Ravencoin and RPC connections.

# This script should work on UNIX and windows systems. I specifically avoided using imports that are not cross compatible.

# The intent is to assist issuers of assets to more easily issue Ravencoin assets as rewards to existing holders of tokens.

# Install the required depends using the below. On windows systems ensure python is installed. 

# Tested using Python version 3.10. 

# Ensure the below dependancies are available.

# pip install bitcoinrpc
# pip install sqlite3

import os
import logging
import getpass
import sqlite3
from tqdm import tqdm
from decimal import Decimal
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

# Ravencoin "raven.config" variables for the user to configure.

# Typical raven.config setting below for mainnet to assist in setting upo the rpc connection. User ideally should change after the = to match your desired information.
# rpcuser=ravencoinlover
# rpcpassword=kakawww
# rpcallowip=127.0.0.1
# rpcport=8766

# Update the below to match your raven.config file to allow the python script and node to communicate.
rpc_user = 'ravencoinlover'
rpc_pass = 'kakawww'
rpc_port = 8766 # default port for main net. 18766 for testnet.

# Set the reward asset name and other settings
rewards_asset_name = "INDYBOWS/THANK_YOU" # This is the reward asset to be rewarded to existing token holders.
asset_names_to_find = ["INDYBOWS"] # This is the asset that will be searched to find asset holders. This could be a main or sub asset. Always use the full "MAINASSET/SUBASSET" 
reward_asset_qty_per_reward_asset = Decimal(2) # This is the quantity of the reward to be issued to each of the valid asset holders. Change the number to reward the actual quantity per held asset. 

# Burn address list to check that no rewarded assets go to burn addresses. User could add addresses here that they want to exclude from rewards.
BURN_ADDRESSES = [
    "RXissueAssetXXXXXXXXXXXXXXXXXhhZGt",
    "RXBurnXXXXXXXXXXXXXXXXXXXXXXWUo9FV",
    "RXReissueAssetXXXXXXXXXXXXXXVEFAWu",
    "RXissueSubAssetXXXXXXXXXXXXXWcwhwL",
    "RXissueUniqueAssetXXXXXXXXXXWEAe58",
    "RXissueQuaLifierXXXXXXXXXXXXUgEDbC",
    "RXissueRestrictedXXXXXXXXXXXXzJZ1q",
    "RXissueMsgChanneLAssetXXXXXXSjHvAY",
    "RXissueSubQuaLifierXXXXXXXXXVTzvv5",
    "RXaddTagBurnXXXXXXXXXXXXXXXXZQm5ya",
    "RVNAssetHoLeXXXXXXXXXXXXXXXXZCEMy6",
    "n1issueAssetXXXXXXXXXXXXXXXXWdnemQ",
    "n1issueQuaLifierXXXXXXXXXXXXUysLTj",
    "n1issueRestrictedXXXXXXXXXXXXZVT9V",
    "n1issueSubAssetXXXXXXXXXXXXXbNiH6v",
    "n1ReissueAssetXXXXXXXXXXXXXXWG9NLd",
    "n1issueMsgChanneLAssetXXXXXXT2PBdD",
    "n1issueSubQuaLifierXXXXXXXXXYffPLh",
    "n1issueUniqueAssetXXXXXXXXXXS4695i",
    "n1addTagBurnXXXXXXXXXXXXXXXXX5oLMH",
    "n1BurnXXXXXXXXXXXXXXXXXXXXXXU1qejP"
]

# Set up logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format='%(asctime)s - %(levelname)s - %(message)s',  # Set the logging format
    handlers=[
        logging.StreamHandler(),  # Print log messages to the console
        logging.FileHandler('rewards.log', mode='w')  # Save log messages to the log file
    ]
)

# Connect to the SQLite database
conn = sqlite3.connect('rewards.db')

# Create an RPC connection to the Ravencoin Core wallet
rpc_connection = AuthServiceProxy("http://{}:{}@localhost:{}".format(rpc_user, rpc_pass, rpc_port))

# Function to test the rpc connection and verify node blockchain sync progress.
def test_rpc_connection(rpc_connection):
    try:
        blockchain_info = rpc_connection.getblockchaininfo()
        chain = blockchain_info.get("chain")
        verification_progress = blockchain_info.get("verificationprogress")

        if verification_progress <= 0.98:
            logging.warning("RPC connection successful, but verification progress is not sufficient.")
            return False

        network = chain
        logging.info(f"RPC connection to {network} network successful.")
        return True

    except JSONRPCException as e:
        logging.error(f"RPC error occurred: {str(e)}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error occurred: {str(e)}")
        return False


# Function to initialize the database
def initialize_database(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_name TEXT,
            type TEXT,
            divisible INTEGER,
            address TEXT,
            quantity DECIMAL,
            txid TEXT
        )
    ''')
    conn.commit()

# Function to fetch the desired asset names from the Ravencoin node.
def fetch_reward_assets(conn, rpc_connection, asset_names_to_find):
    # Ensure asset_names_to_find is a list
    if not isinstance(asset_names_to_find, list):
        asset_names_to_find = [asset_names_to_find]

    # Convert asset_names_to_find list into a single string
    asset_prefix = ",".join([name + "*" for name in asset_names_to_find])

    # Run listassets to get the asset names
    asset_names = rpc_connection.listassets(asset_prefix)

    # Save the asset names in the database
    cursor = conn.cursor()
    for asset_name in asset_names:
        # Check if the asset name already exists in the database
        cursor.execute("SELECT * FROM assets WHERE asset_name = ?", (asset_name,))
        existing_asset = cursor.fetchone()

        if existing_asset:
            # Asset name already exists, skip inserting/updating
            continue

        # Retrieve addresses and asset quantities for the asset name
        asset_info = rpc_connection.listaddressesbyasset(asset_name)
        for address, quantity in asset_info.items():
            # Check if the asset name, address, and quantity already exist in the database
            cursor.execute("SELECT * FROM assets WHERE asset_name = ? AND address = ? AND quantity = ?",
                           (asset_name, address, quantity))

            if cursor.fetchone():
                # Asset name, address, and quantity already exist, skip inserting/updating
                continue

            # Insert the asset name, address, and quantity into the database
            cursor.execute("INSERT INTO assets (asset_name, address, quantity) VALUES (?, ?, ?)",
                           (asset_name, address, quantity))

    conn.commit()

    # Delete asset_names without any quantity or addresses or that are in the BURN_ADDRESSES list
    cursor.execute("DELETE FROM assets WHERE id NOT IN (SELECT id FROM assets WHERE quantity > 0 OR address IS NOT NULL) OR asset_name IN ({})".format(",".join(["?"] * len(BURN_ADDRESSES))), BURN_ADDRESSES)
    conn.commit()

# Function to display the current list of reward assets
def display_reward_assets(conn):
    logger = logging.getLogger(__name__)

    cursor = conn.cursor()
    cursor.execute("SELECT asset_name, CASE WHEN type IS NULL OR type = 'unrewarded' THEN 'Unrewarded Asset' ELSE 'Rewarded Asset' END AS asset_type FROM assets")
    reward_assets = cursor.fetchall()

    printed_assets = set()
    if reward_assets:
        logger.info("Current Reward Assets:")
        for index, asset in enumerate(reward_assets, start=1):
            asset_name = asset[0]
            asset_type = asset[1]
            if asset_name not in printed_assets:
                printed_assets.add(asset_name)
                logger.debug("{}. {} - {}".format(index, asset_name, asset_type))
    else:
        logger.info("No reward assets found.")

    cursor.close()

# Function to edit the type of specific assets between rewarded and unrewarded.
def edit_asset_type(conn, asset_to_edit, new_type):
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE assets SET type = ? WHERE asset_name = ?", (new_type, asset_to_edit))
        conn.commit()
        logging.info(f"Asset type updated successfully. Asset: {asset_to_edit}, New Type: {new_type}")
    except Exception as e:
        logging.error(f"Error updating asset type. Asset: {asset_to_edit}, New Type: {new_type}, Error: {e}")
        conn.rollback()  # Roll back the transaction in case of error
    finally:
        cursor.close()

# Function to get planned transfers marked as rewarded in the database.
def get_planned_transfers(conn, reward_asset_qty_per_reward_asset):
    planned_transfers = {}

    cursor = conn.cursor()
    cursor.execute(
        "SELECT asset_name, address, quantity FROM assets WHERE type = 'rewarded' AND txid IS NULL")
    asset_info = cursor.fetchall()

    for row in asset_info:
        asset_name, address, quantity = row

        if asset_name not in planned_transfers:
            planned_transfers[asset_name] = []

        transfer_info = {
            'asset_name': asset_name,
            'address': address,
            'quantity': quantity * reward_asset_qty_per_reward_asset
        }

        # Check for duplicate entries
        if transfer_info not in planned_transfers[asset_name]:
            planned_transfers[asset_name].append(transfer_info)

    cursor.close()
    return planned_transfers

# Function to display planned transfers to the user.
def display_planned_transfers(planned_transfers, rewards_asset_name):
    logging.info("Planned Transfers:")
    for asset_name, transfers in planned_transfers.items():
        logging.info(f"Asset: {asset_name}")
        for transfer in transfers:
            address = transfer['address']
            quantity = transfer['quantity']
            logging.info(f" - To: {address}")
            logging.info(f"   Quantity: {quantity} {rewards_asset_name}")


# Function to save the txid to the database (this needs additional testing and better logging and error handling)
def update_txid(conn, asset_name, address, adjusted_quantity, txid):
    cursor = conn.cursor()

    sql = "UPDATE assets SET txid = ? WHERE asset_name = ? AND address = ? AND quantity = ? AND type = 'rewarded'"
    values = (txid, asset_name, address, str(adjusted_quantity))  # Convert quantity to string

    try:
        cursor.execute(sql, values)
        conn.commit()
        logging.debug("Transaction ID updated successfully.")
    except Exception as e:
        logging.error("Error updating transaction ID: {}".format(str(e)))
        conn.rollback()
        raise
    finally:
        cursor.close()

# Function to distribute rewards.
def distribute_rewards(conn, rpc_connection, rewards_asset_name, planned_transfers):
    txid_list = []

    total_transfers = sum(len(transfers) for transfers in planned_transfers.values())

    # Create the progress bar without displaying it immediately
    progress_bar = tqdm(total=total_transfers, desc='Distributing rewards', unit='transfer', leave=False, bar_format='{l_bar}{bar}{r_bar}')

    for asset_name, transfer_list in planned_transfers.items():
        logging.info("Asset Being Rewarded: {}".format(asset_name))
        logging.info("Reward Asset: {}".format(rewards_asset_name))

        for transfer_info in transfer_list:
            logging.debug("Address: {}".format(transfer_info['address']))
            logging.debug("Quantity: {}".format(transfer_info['quantity']))

            try:
                wallet_info = rpc_connection.getwalletinfo()
                unlocked_until = wallet_info.get("unlocked_until")

                if unlocked_until is None or unlocked_until > 0:
                    logging.debug("Wallet is unlocked.")
                else:
                    logging.info("Wallet is locked. Please provide the password to unlock the wallet.")
                    password = getpass.getpass("Enter the wallet password: ")

                    # Unlock the wallet
                    rpc_connection.walletpassphrase(password, 60)  # Unlock for 60 seconds (adjust as needed)
                    logging.info("Wallet unlocked successfully.")

                txid = str(rpc_connection.transfer(rewards_asset_name, transfer_info["quantity"], transfer_info["address"]))
                txid_list.append(txid)
                logging.debug("Transaction ID: {}".format(txid))

                # Calculate the adjusted quantity back to the database quantity for matching transactions.
                adjusted_quantity = Decimal(transfer_info["quantity"]) / Decimal(reward_asset_qty_per_reward_asset)

                # Save the txid to the database
                update_txid(conn, asset_name, transfer_info["address"], Decimal(adjusted_quantity), txid)

            except Exception as e:
                logging.error("Error transferring {} to {}: {}: {}".format(
                    transfer_info["quantity"], rewards_asset_name, transfer_info["address"], str(e)))

                # Raise the exception to halt further processing if desired
                raise

            progress_bar.update(1)

    # Close the progress bar after all transfers
    progress_bar.close()

    # Print an empty line to separate the progress bar from the subsequent output
    print()

    return txid_list

# Function to save a receipt file of transactions carried out in a format that most people can access. A .txt file. 
# (This needs additional logging and better error handling)
def save_receipt_file(txid_list, planned_transfers, rewards_asset_name):
    file_path = os.path.join(os.getcwd(), "receipt.txt")

    with open(file_path, "w") as file:
        for asset_name, transfer_info in planned_transfers.items():
            file.write("Asset Being Rewarded: {}\n".format(asset_name))
            file.write("Reward Asset: {}\n".format(rewards_asset_name))
            for transfer in transfer_info:
                file.write("Address: {}\n".format(transfer["address"]))
                file.write("Quantity: {}\n".format(transfer["quantity"]))
            file.write("\n")
        file.write("Transaction IDs:\n")
        for txid in txid_list:
            file.write("{}\n".format(txid))

def has_existing_txids(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM assets WHERE txid IS NOT NULL")
    count = cursor.fetchone()[0]
    cursor.close()
    return count > 0

def clear_txids(conn):
    cursor = conn.cursor()
    cursor.execute("UPDATE assets SET txid = NULL WHERE txid IS NOT NULL")
    conn.commit()
    cursor.close()

# Main function
def main():
    # Initialize the database if necessary
    initialize_database(conn)

    # Test the RPC connection
    if not test_rpc_connection(rpc_connection):
        # Close the database connection
        return

    # Get asset info.
    fetch_reward_assets(conn, rpc_connection, asset_names_to_find)

    # Display current reward assets
    display_reward_assets(conn)

    # Check if there are any existing txids
    if has_existing_txids(conn):
        while True:
            choice = input("Existing transaction IDs found. Do you want to continue the existing distribution? (Y/N): ")
            if choice.upper() == 'Y':
                # Continue with existing distribution
                logging.info("Continuing with existing distribution.")
                break
            elif choice.upper() == 'N':
                # Abandon existing distribution
                logging.info("Abandoning existing distribution.")
                clear_txids(conn)
                break
            else:
                logging.info("Invalid option. Please choose 'Y' or 'N'.")
    else:
        # Prompt the user to update all asset types at once
        all_asset_types = input("Enter 'r' for rewarded or 'u' for unrewarded to update all asset types: ")
        if all_asset_types.lower() == 'r':
            new_type = 'rewarded'
        elif all_asset_types.lower() == 'u':
            new_type = 'unrewarded'
        else:
            logging.info("Invalid option. Skipping updating all asset types.")

        # Update all asset types at once
        if new_type:
            cursor = conn.cursor()
            cursor.execute("UPDATE assets SET type = ?", (new_type,))
            conn.commit()
            cursor.close()

        # Display updated reward assets list
        display_reward_assets(conn)

    # Prompt the user to update individual asset types
    asset_names_input = input("Enter asset names to update their types (separated by commas) (or enter 'c' to continue): ")

    if asset_names_input.lower() == 'c':
        # Display updated reward assets list
        display_reward_assets(conn)
    else:
        asset_names = [name.strip() for name in asset_names_input.split(",")]

        for asset_name in asset_names:
            new_type = input("Enter the new type for asset '{}' (r for rewarded/u for unrewarded): ".format(asset_name))

            # Validate new_type input
            if new_type.lower() not in ['r', 'u']:
                logging.info("Invalid option. Skipping updating asset type.")
                continue

            # Map new_type input to rewarded or unrewarded
            new_type = 'rewarded' if new_type.lower() == 'r' else 'unrewarded'

            # Update asset type
            edit_asset_type(conn, asset_name, new_type)

    # Display updated reward assets list
    display_reward_assets(conn)

    # Get planned transfers
    planned_transfers = get_planned_transfers(conn, reward_asset_qty_per_reward_asset)

    # Display planned transfers
    display_planned_transfers(planned_transfers, rewards_asset_name)

    # Prompt the user to confirm the distribution
    confirmation = input("Confirm the distribution of planned transactions (Y/N): ")
    if confirmation.upper() == "Y":
        # Distribute rewards
        txid_list = distribute_rewards(conn, rpc_connection, rewards_asset_name, planned_transfers)

        # Save receipt file
        save_receipt_file(txid_list, planned_transfers, rewards_asset_name)

# This makes the main function the focus of the script if the user runs it directly but does allow the initial variables to interact.
if __name__ == '__main__':
    try:
        main()
        logging.info("Script executed successfully.")
    except Exception as e:
        logging.error(f"Unexpected error occurred during script execution: {str(e)}")
