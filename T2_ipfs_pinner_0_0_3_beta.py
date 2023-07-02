# Tool 002 - IPFS Pinner Script

# This script is to help people generally more easily bridge between local nodes (or servers) running on the same machine.

# Version: 0.0.3 beta
# Initial Author: LSJI07
# Contributor: 

# Update 0.0.2
# Revised retrieve_small asset function.
## More informative logging and initial database revised to check includes file extension, pinned and added statuses. If present the script moves on.
# Revised retrieve_large asset function.
## More informative logging and initial database revised to check includes file extension, pinned and added statuses. If present the script moves on.
## Updated the script to run dag get before trying to pin add the contents.
## Update the script to honour the max file size threshold when retrieving large files.
## Update the function to give a brief timeout error and longer traceback on other errors.

# Update 0.0.3
# Updated the saved block function logic as that was not updating when restarting the script.

# General Information.
# The script takes some user input and setup and is intended to help most computer minded people setup the node local communications over RPC. 

# When running the script, it will ask the user questions and setup a config file specific to that machine and user in the folder the script is run in.

# This script should work on UNIX and windows systems. I specifically avoided using imports that are not cross compatible.

# The intent is to assist users to pin IPFS content directly from their Ravencoin node to their local IPFS node.

# Install the required depends using the below. On windows systems ensure python is installed.

# Tested using Python version 3.10. 

# Ensure the below dependancies are available.

# pip install bitcoinrpc
# pip install sqlite3

import sys
import argparse
import zmq
import struct
import binascii
import os
import logging # for info and debugging management.
import time # for debugging.
import bitcoinrpc
import traceback
import sqlite3
import select
import msvcrt  # For Windows
import signal  # For Unix-like systems
import json # For error handling
import re # for manipulating regular expressions.
import socket # Used to check for valid ipv4 addresses in configuration.
import requests # Used for making HTTP requests to the local IPFS node.
from tqdm import tqdm # for progress indication.
import configparser # for helping set ipfspinner.config on first run and thereafter.
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
from datetime import datetime # for timestamping logs.
from requests.exceptions import RequestException

# Global variable to track script pause state
paused = False

# Signal handler for Ctrl+C on Unix-like systems
def handle_stop_signal(signal, frame):
    global paused
    if paused:
        sys.exit(0)
    else:
        paused = True
        print("\nPausing the script... Press 's' again to resume.")

# Function to handle keyboard input on Unix-like systems
def handle_keyboard_input_unix(args):
    global paused
    while True:
        if not paused:
            # Wait for keyboard input
            r, _, _ = select.select([sys.stdin], [], [], 1)
            if r:
                key = sys.stdin.readline().strip()
                if key == 's':
                    paused = True
                    print("\nPausing the script... Press 's' again to resume.")
        else:
            # Wait for keyboard input
            r, _, _ = select.select([sys.stdin], [], [], 1)
            if r:
                key = sys.stdin.readline().strip()
                if key == 's':
                    paused = False
                    print("\nResuming the script...")
                    break

# Function to handle keyboard input on Windows
def handle_keyboard_input_windows():
    global paused
    if msvcrt.kbhit():
        key = msvcrt.getch()
        if key == b's':
            if paused:
                paused = False
                print("Resuming the script...")
            else:
                paused = True
                print("Pausing the script... Press 's' again to resume.")
        elif key == b'q':
            print("Stopping the script...")
            sys.exit(0)

def is_valid_ipv4_address(address):
    try:
        socket.inet_pton(socket.AF_INET, address)
    except AttributeError:
        try:
            socket.inet_aton(address)
        except socket.error:
            return False
        return address.count('.') == 3
    except socket.error:
        return False
    return True

def setup_config_settings():
    logger = logging.getLogger("ipfspinner")
    config = configparser.ConfigParser()

    if not os.path.exists('ipfspinner.config'):
        print(r"""
                    @@@@@@@@@@@@@@@@@@@@@@@@@@@@((((((@@@@@@@@@@@@@@@               
                    @@@@@@@@@@@@@@@@@@@@@@@(((((((***@@(@@@@@@@@@@@@@               
                    @@@@@@@@@@@@@@@@@@@@(((((((@((((%***%*****@@@@@@@               
                    @@@@@@@@@@@@@@@@@@@%%%%%%%%@(@%%%@@@((*********@@               
                    @@@@@@@@@@@@@@@@@@%%%@(((@%%%%%%%%%%%%@@@@@@@@@@@               
                    @@@@@@@@@@@@@@@@@(((((&%%%%%%%%@@******@@@@@@@@@@               
                    @@@@@@@@@@@@@@@@((@%%%%@@***************@@@@@@@@@               
                    @@@@@@@@@@@@@@@@************************@@@@@@@@@               
                    @@@@@@@@@@@@@%%(((((((((((((*************@@@@@@@@               
                    @@@@@@@@@@@@@%@%(((((((((((((((((((((((((@@@@@@@@               
                    @@@@@@@@@@@@%%%%%((((((((((((((((((((((((%%%@@@@@               
                    @@@@@@@@@@@@%%%@%%(((((((((((((((((((((((%%%%@@@@               
                    @@@@@@@@@@@%%%%@%%%@((((((((((((((((((((@%%%%@@@@               
                    @@@@@@@@@@@%%%%%%%%%@(((((((((((((((((((%%%%@@@@@               
                    @@@@@@@@@@%%%%%%@%%%%@(((((((((((((((((@%%%@@@@@@               
                    @@@@@@@@@%%%%%%%%%%%%%@((((((((((((((((@%%%@@@@@@               
                    @@@@@@@@@%%%%%%%%@%%%%%@(((((((((((((((%%%@@@@@@@               
                    @@@@@@@@%%%%%%%%%%%%%%%%%(((((((((((((@%%@@@@@@@@               
                    @@@@@@@@%%%%%%%%%%%%%%%%%%((((((((((((%%%@@@@@@@@               
                    @@@@@@@%%%%%%%%%%%@%%%%%%%%(((((((((((%%@@@@@@@@@               
                    @@@@@@@%%%%%%%%%%%%%%%%%%%%%(((((((((@%@@@@@@@@@@               
                    @@@@@@%%%%%%%%%%%%%@%%%%%%%%%@(((((((%%@@@@@@@@@@               
                    @@@@@@%%%%%%%%%%%%%%%%%%%%%%%%@(((((&%@@@@@@@@@@@               
                    @@@@@%%%%%%%%%%%%%%%%%%%%%%%%%%@((((@@@@@@@@@@@@@               
                    @@@@@%%%%%%%%%%%%%%%@%%%%%%%%%%%@(((%@@@@@@@@@@@@               
                    @@@@%%%%%%%%%%%%%%%%%%%%%%%%%%%%%@(@@@@@@@@@@@@@@               
                    @@@%%%%%%%%%%%%%%%%%%@@@@@@@@@@@@@@@@@@@@@@@@@@@@               
                    @@@%%%%%%%%%%%%%%%@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@               
                    @@%%%%%%%%%%%%@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@               
                    @@%%%%%%%%%@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@               
                    @%%%%%%@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@               
                    @%%%@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@               
                             (Script courtesy of RavenAngels)
                             Made by community for community.
                    """)
        print()
        print("Welcome to the Ravencoin IPFS Pinner. Let's get started!")
        print()
        print()
        print("We need to get your IPFS Pinner Settings configured.")
        print()

        config = prompt_ravencoin_settings(config)
        prompt_ravencoin_mode(config)
        prompt_ravencoin_rpc(config)
        prompt_ipfs_settings(config)

        print()
        print("You can view the ipfspinner.config information in the generated file. Delete the ipfspinner.config file to reset the config information and reset this initial configuration process.")
        print()
        with open('ipfspinner.config', 'w') as f:
            config.write(f)
    else:
        config.read('ipfspinner.config')

    return config

def prompt_ravencoin_settings(config):
    print()
    print("Welcome to the Ravencoin IPFS Pinner. Let's get started!")
    print()
    print()
    print("We need to get your IPFS Pinner Settings configured.")
    print()
    print("Enter 'true', 'false' or the required setting. 'default' is also valid.")
    print()

    config['ipfspinner'] = {}

    noblockscan_input = input("01. Ravencoin node block scanning over RPC for Asset Information (True/False/Default): ")
    if noblockscan_input.lower() == 'true':
        config['ipfspinner']['noblockscan'] = 'True'
    elif noblockscan_input.lower() == 'false':
        config['ipfspinner']['noblockscan'] = 'False'
    elif noblockscan_input.lower() == 'default':
        config['ipfspinner']['noblockscan'] = 'False'
    else:
        print("Invalid input. You must enter 'True', 'False', or 'Default'")

    print()
    print("*INFO* You must run ravend or raven-qt using '--zmqpubrawtx=tcp://127.0.0.1:28766' as well as an IPFS node for ongoing digital asset IPFS hash monitoring: ")
    print()

    config['ipfspinner'] = {}

    nozmqwatch_input = input("02. Ravencoin node using ZMQ to watch Assets and pass new asset IPFS hash information (true/false/default): ")
    if nozmqwatch_input.lower() == 'true':
        config['ipfspinner']['nozmqwatch'] = 'True'
    elif nozmqwatch_input.lower() == 'false':
        config['ipfspinner']['nozmqwatch'] = 'False'
    elif nozmqwatch_input.lower() == 'default':
        config['ipfspinner']['nozmqwatch'] = 'False'
    else:
        print("Invalid input. You must enter 'True', 'False', or 'Default'")

    config['ipfspinner'] = {}

    safemode_input = input("03. Safe mode (True/False/Default): ")
    if safemode_input.lower() == 'true':
        config['ipfspinner']['safemode'] = 'True'
    elif safemode_input.lower() == 'false':
        config['ipfspinner']['safemode'] = 'False'
    elif safemode_input.lower() == 'default':
        config['ipfspinner']['safemode'] = 'False'
    else:
        print("Invalid input. You must enter 'True', 'False', or 'Default'")

    config['ipfspinner'] = {}

    debug_input = input("04. Debug mode (True/False/Default): ")
    if debug_input.lower() == 'true':
        config['ipfspinner']['debug'] = 'True'
    elif debug_input.lower() == 'false':
        config['ipfspinner']['debug'] = 'False'
    elif debug_input.lower() == 'default':
        config['ipfspinner']['debug'] = 'False'
    else:
        print("Invalid input. You must enter 'True', 'False', or 'Default'")

    while True:
        block_input = input("05. Enter the block to start scanning Ravencoin FROM for the initial sync (default block: 435456 for Mainnet, testnet varies): ")
        if block_input == '':
            block_input = '435456'
        elif block_input.lower() == 'default':
            block_input = '435456'
        elif not block_input.isdigit():
            print("Invalid input. Block must be a number.")
            continue

        config['ipfspinner']['block'] = block_input
        break

    while True:
        folder_input = input("06. Input folder path to save files related to digital asset to (default: /ipfsfiles): ")
        if folder_input == '':
            folder_input = '/ipfsfiles'
        elif folder_input.lower() == 'default':
            folder_input = '/ipfsfiles'
        elif not os.path.isdir(folder_input):
            print("Invalid input. Folder path must be an existing directory.")
            continue

        config['ipfspinner']['folder'] = folder_input
        break

    while True:
        threshold_input = input("07. Enter the new filesize waterline in MB to differentiate between large and small files. (default is circa 15MB): ")
        if threshold_input == '':
            threshold_input = '15'
        elif not threshold_input.isdigit():
            print("Invalid input. Threshold must be a number.")
            continue

        threshold_bytes = int(threshold_input) * 1024 * 1024  # Convert MB to bytes
        config['ipfspinner']['FILESIZE_THRESHOLD'] = str(threshold_bytes)
        break

    while True:
        large_threshold_input = input("08. Enter the new max large file threshold in MB (default is 500MB): ")
        if large_threshold_input == '':
            large_threshold_input = '500'
        elif not large_threshold_input.isdigit():
            print("Invalid input. Threshold must be a number.")
            continue

        large_threshold_bytes = int(large_threshold_input) * 1024 * 1024  # Convert MB to bytes
        config['ipfspinner']['MAX_LARGE_FILE_THRESHOLD'] = str(large_threshold_bytes)
        break

    return config


def prompt_ravencoin_mode(config):
    print("Ravencoin Node Settings")
    while True:
        mode_input = input("01. Mainnet or Testnet mode? Enter 'default' or 'testnet': ")
        if mode_input == '':
            mode_input = ''
        elif mode_input.lower() == 'default':
            mode_input = ''
        elif mode_input.lower() == 'testnet':
            mode_input = '-testnet'
        elif mode_input.lower() == '-testnet':
            pass
        else:
            print("Invalid input. Enter '-testnet' for testnet, 'default' for main.")
            continue

        config['ravencoin'] = {}
        config['ravencoin']['mode'] = mode_input
        break


def prompt_ravencoin_rpc(config):
    print("***INFO***")
    print()
    print("*INFO* The below settings must be entered exactly as per raven.config.")
    print("*INFO* We do recommend the the below indexes are enable in your Ravencoin node in raven.config to allow the node access to the full dataset")
    print("*INFO* raven.config can be found in raven-qt by going to the following menus and options.")
    print("'Menu (Wallet) > Menu (Options) > Tab (Window) > Button (Open Configuration File)'")
    print()
    print("txindex=1")
    print("assetindex=1")
    print("timestampindex=1")
    print("addressindex=1")
    print("spentindex=1")
    print()
    print("*INFO* We do recommend the the below are enabled, or '=1', in Ravencoin node using raven-qt node in raven.config to enable RPC calls. This is not normally required using ravend.")
    print()
    print("server=1")
    print()
    print("*INFO* We would suggest you consider adding 'rpcallowip' as well. Most people will be running RPC on the local machine and this binds the RPC node to the localhost.") 
    print()
    print("rpcallowip=127.0.0.1")
    print()
    print("*INFO* Note this script is designed to be used on a single machine and interface between the local Ravencoin node and local IPFS node.")
    print()
    print("*INFO* We do not recommend using unencrypted RPC over the external internet as user names and passwords would be exposed.")
    print()

    while True:
        rpc_port_input = input("02. Enter the Ravencoin node rpcport (default for mainnet: 8766): ")
        if rpc_port_input == '':
            rpc_port_input = '8766'
        elif rpc_port_input.lower() == 'default':
            rpc_port_input = '8766'
        elif not rpc_port_input.isdigit() or int(rpc_port_input) not in range(1, 65536):
            print("Invalid input. Port number must be an integer between 1 and 65535.")
            continue

        config['ravencoin']['rpc_port'] = rpc_port_input
        break

    while True:
        rpc_user_input = input("03. Enter the rpcuser: ")
        if rpc_user_input == '':
            print("Invalid input. rpcuser cannot be empty.")
            continue

        rpc_pass_input = input("04. Enter the rpcpassword: ")
        if rpc_pass_input == '':
            print("Invalid input. rpcpassword cannot be empty.")
            continue

        config['ravencoin']['rpc_user'] = rpc_user_input
        config['ravencoin']['rpc_pass'] = rpc_pass_input
        break


def prompt_ipfs_settings(config):
    print()
    print("Now we need to set the IPFS Node Settings")
    print()

    config['ipfs'] = {}

    while True:
        ipfs_host_input = input("01. Enter the IPFS node IP address (default: localhost): ")
        if ipfs_host_input == '':
            ipfs_host_input = 'localhost'
        elif ipfs_host_input.lower() == 'default':
            ipfs_host_input = 'localhost'
        elif not is_valid_ipv4_address(ipfs_host_input):
            print("Invalid input. IP address must be in IPv4 format or 'default'.")
            continue

        config['ipfs']['host'] = ipfs_host_input
        break

    while True:
        ipfs_port_input = input("02. Enter the IPFS port (default: 5001): ")
        if ipfs_port_input == '':
            ipfs_port_input = '5001'
        elif ipfs_port_input.lower() == 'default':
            ipfs_port_input = '5001'
        elif not ipfs_port_input.isdigit() or not (0 <= int(ipfs_port_input) <= 65535):
            print("Invalid input. Port must be an integer between 0 and 65535.")
            continue

        config['ipfs']['port'] = ipfs_port_input
        break

    while True:
        ipfs_timeout_input = input("03. Enter the desired timeout to be used when waiting for IPFS files to retrieve. In seconds (default: 60): ")
        if ipfs_timeout_input == '':
            ipfs_timeout_input = '60'
        elif ipfs_timeout_input.lower() == 'default':
            ipfs_timeout_input = '60'
        elif not ipfs_timeout_input.isdigit() or int(ipfs_timeout_input) <= 0:
            print("Invalid input. Timeout must be a positive integer or 'default'.")
            continue

        config['ipfs']['timeout'] = ipfs_timeout_input
        break


def read_config_settings():
    logger = logging.getLogger("ipfspinner")
    config = configparser.ConfigParser()
    config.read('ipfspinner.config')

    settings = {}

    settings['noblockscan'] = config.getboolean('ipfspinner', 'noblockscan', fallback=False)
    settings['nozmqwatch'] = config.getboolean('ipfspinner', 'nozmqwatch', fallback=False)
    settings['safemode'] = config.getboolean('ipfspinner', 'safemode', fallback=False)
    settings['FIRST_ASSET_BLOCK'] = config.getint('ipfspinner', 'block', fallback=435456)
    settings['folder'] = config.get('ipfspinner', 'folder', fallback=None)
    settings['debug'] = config.getboolean('ipfspinner', 'debug', fallback=False)
    settings['mode'] = config.get('ravencoin', 'mode', fallback='')
    settings['rpc_port'] = config.getint('ravencoin', 'rpc_port', fallback='8766')
    settings['rpc_user'] = config.get('ravencoin', 'rpc_user', fallback='rpcuser')
    settings['rpc_pass'] = config.get('ravencoin', 'rpc_pass', fallback='rpcpass555')
    settings['ipfs_host'] = config.get('ipfs', 'host', fallback='localhost')
    settings['ipfs_port'] = config.getint('ipfs', 'port', fallback=5001)
    settings['ipfs_timeout'] = int(config.get('ipfs', 'timeout', fallback=60))
    settings['filesize_threshold'] = int(config.get('ipfspinner', 'FILESIZE_THRESHOLD', fallback=15000000))

    return settings

logger = logging.getLogger("ipfspinner")
config = setup_config_settings()

# INFO Ravencoin RPC functions
def get_rpc_connection(config):
    from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
    rpc_user = config['ravencoin']['rpc_user']
    rpc_pass = config['ravencoin']['rpc_pass']
    rpc_port = config['ravencoin']['rpc_port']
    connection = "http://%s:%s@127.0.0.1:%s" % (rpc_user, rpc_pass, rpc_port)
    rpc_conn = AuthServiceProxy(connection)
    return rpc_conn

rpc_connection = get_rpc_connection(config)

def get_blockinfo(num):
    hash = rpc_connection.getblockhash(num)
    blockinfo = rpc_connection.getblock(hash)
    return(blockinfo)

def get_block(hash):
    blockinfo = rpc_connection.getblock(hash)
    return(blockinfo)

def get_rawtx(tx):
    txinfo = rpc_connection.getrawtransaction(tx)
    return(txinfo)

def get_bci():
    bci = rpc_connection.getblockchaininfo()
    return(bci)    

def decode_rawtx(txdata):
    logger.debug("Decoding transaction: %s", txdata)
    try:
        txjson = rpc_connection.decoderawtransaction(txdata)
        return txjson
    except Exception as e:
        logger.exception("Error occurred while decoding raw transaction: %s", e)
        raise

def decode_script(script):
    try:
        scriptinfo = rpc_connection.decodescript(script)
        return scriptinfo
    except Exception as e:
        logger.exception("Error occurred while decoding script: %s", e)
        raise


# Create a new SQLite database and table
def setup_database():
    dbconn = sqlite3.connect('ravencoin.db')
    dbc = dbconn.cursor()
    dbc.execute('''
        CREATE TABLE IF NOT EXISTS assets (
            id INTEGER PRIMARY KEY,
            block_height INTEGER,
            asset_name TEXT,
            ipfs_hash TEXT,
            filename TEXT,
            file_extension TEXT,
            file_size INTEGER,
            downloaded BOOLEAN,
            added BOOLEAN,
            pinned BOOLEAN,
            UNIQUE(asset_name, ipfs_hash)
        )
    ''')
    dbconn.commit()
    return dbconn, dbc

# Used to show better human readable size indication than bytes in logging.
def format_file_size(size_in_bytes):
    if size_in_bytes < 1024:
        return f"{size_in_bytes} B"
    elif size_in_bytes < 1024 ** 2:
        return f"{size_in_bytes / 1024:.2f} KB"
    elif size_in_bytes < 1024 ** 3:
        return f"{size_in_bytes / (1024 ** 2):.2f} MB"
    elif size_in_bytes < 1024 ** 4:
        return f"{size_in_bytes / (1024 ** 3):.2f} GB"
    else:
        return f"{size_in_bytes / (1024 ** 4):.2f} TB"
    
def estimate_ipfs_storage_size(filesize_threshold, dbc):
    # Read the IPFSpinner configuration from ipfspinner.config file
    config = configparser.ConfigParser()
    config.read("ipfspinner.config")

    dbc.execute('SELECT file_size FROM assets WHERE file_size IS NOT NULL AND file_size <= ?', (config['ipfspinner'].getint('max_large_file_threshold'),))
    file_sizes = dbc.fetchall()

    total_size = sum(file_size[0] for file_size in file_sizes)

    # Apply 10% increase for recursive files and folders
    total_size *= 1.1

    # If the total size is less than the file size threshold, use the file size threshold
    if total_size < filesize_threshold:
        total_size = filesize_threshold

    logging.info(f"Estimated total IPFS storage size: {format_file_size(total_size)}")

def asset_handler(asset_script, block_num, config, args):
    global dbc, dbconn

    asset_file = asset_to_file(asset_script.get('asset_name'))

    try:
        log_asset_details(asset_script, asset_script.get('asset_name'), asset_file)

        if asset_script.get('hasIPFS'):
            ipfs_hash = asset_script.get('ipfs_hash')
            log_ipfs_hash(ipfs_hash)

            start_time = time.time()

            # Open the database connection
            with sqlite3.connect('ravencoin.db') as dbconn:
                dbc = dbconn.cursor()

                # Check if the size is already available in the database
                dbc.execute('SELECT file_size FROM assets WHERE asset_name = ? AND ipfs_hash = ?', (asset_script.get('asset_name'), ipfs_hash))
                result = dbc.fetchone()
                if result is not None and result[0] is not None and result[0] > 0:
                    size = result[0]
                else:
                    # Check if the IPFS hash has been encountered multiple times with None size
                    dbc.execute('SELECT COUNT(*) FROM assets WHERE ipfs_hash = ? AND file_size IS NULL', (ipfs_hash,))
                    count_result = dbc.fetchone()
                    if count_result is not None and count_result[0] >= 2:
                        size = None  # Skip checking the IPFS hash again if encountered multiple times with None size
                    else:
                        size = get_ipfs_file_size(ipfs_hash, config)
                        dbc.execute('''
                            UPDATE assets SET file_size = ? WHERE asset_name = ? AND ipfs_hash = ?
                        ''', (size, asset_script.get('asset_name'), ipfs_hash))
                        dbconn.commit()

                # Format the size for logging
                if size is not None:
                    formatted_size = format_file_size(size)
                    logging.info(f"Size of IPFS file for {asset_script.get('asset_name')}: {formatted_size}")
                else:
                    logging.info(f"Size of IPFS file for {asset_script.get('asset_name')}: Missing")

                filename = generate_file_name(asset_file, ipfs_hash)
                logging.info(f"Filename: {filename}")

                # Insert or update the database record
                dbc.execute('''
                    INSERT INTO assets (block_height, asset_name, ipfs_hash, filename, file_size, downloaded, added, pinned)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(asset_name, ipfs_hash) DO UPDATE SET
                        file_size = excluded.file_size,
                        downloaded = excluded.downloaded,
                        added = excluded.added,
                        pinned = excluded.pinned
                ''', (block_num, asset_script.get('asset_name'), ipfs_hash, filename, size, False, False, False))
                dbconn.commit()

                end_time = time.time()
                elapsed_time = end_time - start_time
                logging.info(f"Elapsed time for {asset_script.get('asset_name')}: {elapsed_time} seconds.")

    except Exception as e:
        logging.error("An error occurred in asset_handler: %s", str(e))
        logging.error(traceback.format_exc())

       
def log_asset_details(asset_script, asset_name, asset_file):
    logging.debug("Type: %s", asset_script.get('type'))
    logging.debug("Asset: %s", asset_name)
    logging.debug("Asset File: %s", asset_file)
    logging.debug(asset_script.get('amount'))
    logging.debug(asset_script.get('units'))
    logging.debug("Reissuable: %s", str(asset_script.get('reissuable')))
    logging.debug("Has IPFS: %s", str(asset_script.get('hasIPFS')))
    
def log_ipfs_hash(ipfs_hash):
    logging.debug(ipfs_hash)

def insert_asset_to_database(asset_name, ipfs_hash, block_num, filename, size):
    dbc.execute('''
        SELECT * FROM assets WHERE asset_name = ? AND ipfs_hash = ?
    ''', (asset_name, ipfs_hash))
    result = dbc.fetchone()

    if result is None:
        file_extension = ""
        dbc.execute('''
            INSERT INTO assets (block_height, asset_name, ipfs_hash, filename, file_extension, file_size, downloaded, added, pinned)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (block_num, asset_name, ipfs_hash, filename, file_extension, size, False, False, False))
        dbconn.commit()
        return True
    else:
        return False


# Retrieval functions

def update_asset_status(asset_name, ipfs_hash, dbc, dbconn, pinned=False, added=False):
    """Update the status of an asset in the database.

    Args:
        asset_name (str): The name of the asset.
        ipfs_hash (str): The IPFS hash of the asset.
        dbc: The database connection object.
        dbconn: The database connection.
        pinned (bool): The status of whether the asset is pinned. Default: False.
        added (bool): The status of whether the asset is added. Default: False.
    """
    dbc.execute('UPDATE assets SET pinned = ?, added = ? WHERE asset_name = ? AND ipfs_hash = ?', (pinned, added, asset_name, ipfs_hash))
    dbconn.commit()

def retrieve_asset(asset_name, ipfs_hash, args, json_only_check, filesize_threshold, dbc, dbconn):
    try:
        if not ipfs_hash.startswith("Qm") or len(ipfs_hash) != 46:
            logging.error(f"Error: Invalid IPFS hash format for asset {asset_name}, IPFS hash {ipfs_hash}. Skipping asset.")
            update_asset_status(asset_name, ipfs_hash, dbc, dbconn, pinned=False, added=False)
            return

        dbc.execute('SELECT file_size FROM assets WHERE ipfs_hash = ?', (ipfs_hash,))
        result = dbc.fetchone()
        if result is not None:
            size = result[0]
        else:
            size = get_ipfs_file_size(ipfs_hash, config)
            dbc.execute('UPDATE assets SET file_size = ? WHERE ipfs_hash = ?', (size, ipfs_hash))
            dbconn.commit()

        if size is not None:
            dbc.execute('SELECT pinned, added FROM assets WHERE ipfs_hash = ?', (ipfs_hash,))
            result = dbc.fetchone()
            if result is not None and result[0] and result[1]:
                logging.debug(f"IPFS hash {ipfs_hash} is already pinned and added. Skipping pinning.")
                return

            pinned_objects = ipfs_pin_ls(ipfs_hash)
            if pinned_objects is not None and 'Keys' in pinned_objects:
                keys = pinned_objects['Keys']
                if ipfs_hash in keys:
                    identical_hashes = [k for k, v in keys.items() if v['Type'] == keys[ipfs_hash]['Type']]
                    dbc.execute('UPDATE assets SET pinned = ?, added = ? WHERE ipfs_hash IN ({})'.format(','.join(['?'] * len(identical_hashes))), (True, True, *identical_hashes))
                    dbconn.commit()
                    logging.info(f"IPFS hash for asset {asset_name} has already been pinned and added.")
                    return
                else:
                    logging.info(f"IPFS hash {ipfs_hash} is not currently pinned. Continuing to retrieve content...")

            if args.safemode:
                print(f"Retrieving small file content for {asset_name} (Safe Mode)...")
                retrieve_small_file(asset_name, ipfs_hash, json_only_check, args, dbc, dbconn)
            else:
                if size <= filesize_threshold:
                    print(f"Retrieving small file content for {asset_name}...")
                    retrieve_small_file(asset_name, ipfs_hash, json_only_check, args, dbc, dbconn)
                else:
                    print(f"Retrieving large file content for {asset_name}...")
                    retrieve_large_file(asset_name, ipfs_hash, dbc, dbconn)
        else:
            logging.error(f"Error: File size is not available for IPFS hash {ipfs_hash}. Skipping asset.")
            update_asset_status(asset_name, ipfs_hash, dbc, dbconn, pinned=False, added=False)

    except IPFSException as e:
        logging.error(f"Error: Failed to retrieve IPFS content for {ipfs_hash}")
        logging.error(str(e))
        update_asset_status(asset_name, ipfs_hash, dbc, dbconn, pinned=False, added=False)

    except Exception as ex:
        logging.error(f"Error: Failed to retrieve IPFS content for {ipfs_hash}")
        logging.error(str(ex))
        update_asset_status(asset_name, ipfs_hash, dbc, dbconn, pinned=False, added=False)


def retrieve_small_file(asset_name, ipfs_hash, json_only_check, args, dbc, dbconn):
    try:
        # Get asset name from the asset script
        asset_name = asset_name

        # Check if the file extension already exists in the database and if it is pinned and added
        dbc.execute('SELECT file_extension, pinned, added FROM assets WHERE asset_name = ? AND ipfs_hash = ?', (asset_name, ipfs_hash))
        result = dbc.fetchone()
        if result is not None:
            file_extension, pinned, added = result
            if file_extension is not None and pinned and added:
                logging.info(f"File contents is marked as {file_extension} and already exists, the asset contents is pinned and added for asset {asset_name} and IPFS hash {ipfs_hash}.")
                return

        # Retrieve content from IPFS using ipfs_dag_get
        try:
            content_bytes = ipfs_dag_get(ipfs_hash)
            content = content_bytes.decode('utf-8')
            logging.debug(f"Content retrieved successfully:\n{content}")
        except IPFSException as e:
            raise Exception(f"Error retrieving content from IPFS: {str(e)}")

        # Fetch IPFS size from the database
        dbc.execute('SELECT file_size FROM assets WHERE asset_name = ? AND ipfs_hash = ?', (asset_name, ipfs_hash))
        result = dbc.fetchone()
        if result is None:
            raise Exception(f"Asset {asset_name} with IPFS hash {ipfs_hash} not found in the database.")

        size = result[0]

        # Determine the content type based on its structure
        if content.startswith(('{', '[')) and content.rstrip().endswith(('}', ']')):
            logging.info("Content is JSON")
            new_file_extension = 'json'
        else:
            logging.info("Content is CAR")
            new_file_extension = 'car'

        # Update the database with the retrieved file extension and mark as added
        logging.info(f"Updating database with file extension {new_file_extension} and marking as added for asset {asset_name} and IPFS hash {ipfs_hash}")
        dbc.execute('UPDATE assets SET file_extension = ?, added = ? WHERE asset_name = ? AND ipfs_hash = ?', (new_file_extension, True, asset_name, ipfs_hash))
        dbconn.commit()

        # Pin the asset if it is not already pinned
        dbc.execute('SELECT pinned FROM assets WHERE asset_name = ? AND ipfs_hash = ?', (asset_name, ipfs_hash))
        result = dbc.fetchone()
        if result is not None and result[0]:
            logging.info(f"Asset {asset_name} with IPFS hash {ipfs_hash} is already pinned.")
        else:
            logging.info(f"Pinning asset {asset_name} with IPFS hash {ipfs_hash}")
            try:
                ipfs_pin_add(ipfs_hash)
                dbc.execute('UPDATE assets SET pinned = ? WHERE asset_name = ? AND ipfs_hash = ?', (True, asset_name, ipfs_hash))
                dbconn.commit()
            except Exception as e:
                logging.error(f"Error pinning asset {asset_name} with IPFS hash {ipfs_hash}: {str(e)}")

    except Exception as e:
        logging.error(f"Error: Unable to retrieve IPFS file for asset {asset_name}. Reason: {str(e)}")

        check_missing_asset_info(asset_name, ipfs_hash)


def retrieve_large_file(asset_name, ipfs_hash, dbc, dbconn):
    try:
        # Read the IPFS configuration from ipfspinner.config file
        config = configparser.ConfigParser()
        config.read("ipfspinner.config")
        ipfs_timeout = config.getint("ipfs", "timeout")
        max_large_file_threshold = int(config.get("ipfspinner", "max_large_file_threshold"))

        ipfs_timeout = int(ipfs_timeout)  # Ensure ipfs_timeout is an integer

        # Check if the file extension already exists in the database and if it is pinned and added
        dbc.execute('SELECT file_extension, pinned, added, file_size FROM assets WHERE asset_name = ? AND ipfs_hash = ?', (asset_name, ipfs_hash))
        result = dbc.fetchone()
        if result is not None:
            file_extension, pinned, added, file_size = result
            if file_extension is not None and pinned and added:
                logging.info(f"File content is already marked as {file_extension}, and the asset is pinned and added for asset {asset_name} and IPFS hash {ipfs_hash}.")
                return

        # Check if the file size exceeds the maximum threshold
        if file_size is not None and file_size > max_large_file_threshold:
            logging.info(f"Skipping retrieval of asset {asset_name} with IPFS hash {ipfs_hash} due to its large size.")
            logging.info(f"File size: {format_file_size(file_size)}, Maximum threshold: {format_file_size(max_large_file_threshold)}")
            dbc.execute('UPDATE assets SET pinned = ?, added = ? WHERE asset_name = ? AND ipfs_hash = ?', (False, False, asset_name, ipfs_hash))
            dbconn.commit()
            return

        # Retrieve file content from IPFS using ipfs_dag_get()
        file_content = ipfs_dag_get(ipfs_hash)

        # Pin the asset if it is not already pinned
        dbc.execute('SELECT pinned FROM assets WHERE asset_name = ? AND ipfs_hash = ?', (asset_name, ipfs_hash))
        result = dbc.fetchone()
        if result is not None and result[0]:
            logging.info(f"Asset {asset_name} with IPFS hash {ipfs_hash} is already pinned.")
        else:
            logging.info(f"Pinning asset {asset_name} with IPFS hash {ipfs_hash}")
            ipfs_pin_add(ipfs_hash)
            dbc.execute('UPDATE assets SET pinned = ? WHERE asset_name = ? AND ipfs_hash = ?', (True, asset_name, ipfs_hash))
            dbconn.commit()

        # Set the file extension to 'car'
        file_extension = 'car'
        logging.info("File content is CAR")

        # Update the database to indicate that the file has been pinned and added
        dbc.execute('UPDATE assets SET pinned = ?, added = ?, file_extension = ? WHERE asset_name = ? AND ipfs_hash = ?', (True, True, file_extension, asset_name, ipfs_hash))
        dbconn.commit()

    except IPFSException as e:
        logging.error(f"Error: Unable to retrieve IPFS file for asset {asset_name}. Reason: {str(e)}")
    except Exception as e:
        logging.error(f"Error: Unable to retrieve IPFS file for asset {asset_name}. Reason: {str(e)}")
        logging.error("Error details:", exc_info=True)

        # Update the database to indicate that the file has not been retrieved
        dbc.execute('UPDATE assets SET pinned = ?, added = ? WHERE asset_name = ? AND ipfs_hash = ?', (False, False, asset_name, ipfs_hash))
        dbconn.commit()



def generate_file_name(asset_file, ipfs_hash):
    filename = asset_file + '_' + ipfs_hash

    # Replace specific characters with their URL-encoded values
    filename = filename.replace('/', '%2F')
    filename = filename.replace('*', '%2A')
    filename = filename.replace('&', '%26')
    filename = filename.replace('?', '%3F')
    filename = filename.replace(':', '%3A')
    filename = filename.replace('=', '%3D')

    # Remove potential special characters from the filename
    filename = re.sub(r'[^\w\.-]', '_', filename)

    return filename


# This function is called when the script is unable to fetch an asset from IPFS and creates a text file list of missing hashes, avoiding duplicates.
def check_missing_asset_info(asset_name, ipfs_hash):
    missing_hashes_path = os.path.join(os.getcwd(), "missing_hashes")
    os.makedirs(missing_hashes_path, exist_ok=True)
    filename = os.path.join(missing_hashes_path, "missing_IPFS_hashes.txt")

    dbc.execute('''
        SELECT downloaded, added, pinned FROM assets WHERE asset_name = ? AND ipfs_hash = ?
    ''', (asset_name, ipfs_hash))
    result = dbc.fetchone()

    if result is not None:
        downloaded, added, pinned = result
    else:
        logging.error(f"Error: Asset {asset_name} with IPFS hash {ipfs_hash} not found in the database.")
        return

    updated_lines = []
    status_line = f"{asset_name}: {ipfs_hash} - Downloaded: {downloaded}, Added: {added}, Pinned: {pinned}"

    if os.path.isfile(filename):
        with open(filename, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    parts = line.split(':')
                    if len(parts) >= 2:
                        file_asset_name, file_ipfs_hash = parts[0].strip(), parts[1].strip()
                        if file_asset_name != asset_name or file_ipfs_hash != ipfs_hash:
                            updated_lines.append(line)

    updated_lines.append(status_line)
    updated_lines.append(f"Size not found for asset {asset_name} with IPFS hash {ipfs_hash}")

    with open(filename, 'w') as f:
        f.write('\n'.join(updated_lines))

    logging.info(f"Updated status for {asset_name}: {ipfs_hash} in {filename}")

# INFO IPFS HTTP based functions

class IPFSException(Exception):
    """Exception raised for errors related to IPFS operations."""
    pass

def add_ipfs_peers(config):
    peers = [
        ("RavencoinIPFS", "12D3KooWBNqVomfLbFk16gdu8azcQEyg6RcRFWfp2QxSztdiC7iM", "/dnsaddr/ravencoinipfs.com/p2p/12D3KooWBNqVomfLbFk16gdu8azcQEyg6RcRFWfp2QxSztdiC7iM"),
        ("MangoFarmAssets", "12D3KooWRjhU28ez8xGN7477oiG67NbUpL9owAkWpwGZct3PdUpe", "/dnsaddr/ravencoinipfs.com/p2p/12D3KooWRjhU28ez8xGN7477oiG67NbUpL9owAkWpwGZct3PdUpe"),
        ("RavencoinIPFS Gateway 1", "12D3KooWSxmtT3azrbtDnadxC196HS6QoU2dNJvvyzAFGfkZPiPB", "/dnsaddr/ravencoinipfs-gateway.com/p2p/12D3KooWSxmtT3azrbtDnadxC196HS6QoU2dNJvvyzAFGfkZPiPB"),
        ("RavencoinIPFS Gateway 2", "12D3KooWAw2gTLa2LXhnx6tkhJcdPvvaYH82R6m8dxttDCPEkCtm", "/dnsaddr/ravencoinipfs-gateway.com/p2p/12D3KooWAw2gTLa2LXhnx6tkhJcdPvvaYH82R6m8dxttDCPEkCtm"),
        ("RavencoinIPFS Gateway 3", "12D3KooWS1ehpyirKUJma8tkVaMennGTCG9E822CXuWXnJ37YBqL", "/dnsaddr/ravencoinipfs-gateway.com/p2p/12D3KooWS1ehpyirKUJma8tkVaMennGTCG9E822CXuWXnJ37YBqL")
    ]

    existing_peers = set()
    try:
        url = f"http://{config['ipfs']['host']}:{config['ipfs']['port']}/api/v0/bootstrap/list"
        response = requests.post(url)
        response.raise_for_status()
        result = response.json()
        if 'Peers' in result:
            existing_peers = set(result['Peers'])
    except requests.exceptions.RequestException as e:
        logging.warning("Failed to retrieve existing IPFS peers from the bootstrap list")
        logging.warning("Error: %s", str(e))

    for peer_name, peer_id, peer_address in peers:
        if peer_address not in existing_peers:
            try:
                url = f"http://{config['ipfs']['host']}:{config['ipfs']['port']}/api/v0/bootstrap/add?arg={peer_address}"
                response = requests.post(url)
                if response.status_code == 200:
                    logging.info(f"Added {peer_name} ({peer_id}) to the bootstrap list")
                else:
                    logging.warning(f"Failed to add {peer_name} ({peer_id}) to the bootstrap list")
                    logging.warning(f"Error: {response.text}")
            except requests.exceptions.RequestException as e:
                logging.warning(f"Failed to add {peer_name} ({peer_id}) to the bootstrap list")
                logging.warning(f"Error: {str(e)}")
                raise IPFSException(f"Failed to add {peer_name} ({peer_id}) to the bootstrap list")
        else:
            logging.info(f"{peer_name} ({peer_id}) is already in the bootstrap list")


def get_ipfs_file_size(ipfs_hash, config):
    MAX_RETRIES = 2
    RETRY_DELAY = 1
    TIMEOUT = 10

    retries = 0
    cumulative_size = 0

    while retries < MAX_RETRIES:
        try:
            logging.info("Checking size in IPFS")
            url = f"http://{config['ipfs']['host']}:{config['ipfs']['port']}/api/v0/object/stat?arg={ipfs_hash}"
            response = requests.post(url, timeout=TIMEOUT)
            response.raise_for_status()
            res = response.json()
            logging.debug(res)
            cumulative_size = res['CumulativeSize']
            break  # Break out of the loop if the size is successfully obtained
        except (requests.exceptions.RequestException, KeyError) as e:
            logging.error("Error checking IPFS file size: %s", str(e))
            retries += 1
            if retries < MAX_RETRIES:
                logging.info(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                logging.info("Maximum number of retries reached. Couldn't get size - skipping asset.")
    
    return cumulative_size if cumulative_size > 0 else None


def ipfs_cat(ipfs_hash):
    """Retrieve file content from IPFS using the 'cat' command.

    Args:
        ipfs_hash (str): The IPFS hash of the file.

    Returns:
        str: The contents of the file.

    Raises:
        Exception: If an error occurs while retrieving the file content.
    """
    logging.info("Retrieving file content from IPFS using 'cat'...")

    config = configparser.ConfigParser()
    config.read("ipfspinner.config")
    ipfs_host = config.get("ipfs", "host")
    ipfs_port = config.getint("ipfs", "port")
    ipfs_timeout = config.getint("ipfs", "timeout")

    url = f"http://{ipfs_host}:{ipfs_port}/api/v0/cat"
    params = {
        'arg': ipfs_hash,
        'offset': 0,
        'progress': True
    }
    try:
        response = requests.post(url, params=params, timeout=ipfs_timeout)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        error_message = None
        if hasattr(e, 'response') and e.response is not None and e.response.text is not None:
            error_message = e.response.text
        raise Exception(f"Error retrieving file content from IPFS using 'cat': {error_message or str(e)}")

def ipfs_get(ipfs_host, ipfs_port, ipfs_timeout, ipfs_hash):
    """Get a file from IPFS.

    Args:
        ipfs_host (str): The IPFS host.
        ipfs_port (int): The IPFS port.
        ipfs_timeout (int): The timeout in seconds.
        ipfs_hash (str): The IPFS hash of the file.

    Returns:
        bytes: The contents of the file.

    Raises:
        IPFSException: If an error occurs while getting the file.
    """
    logging.debug("Getting from IPFS")
    url = f"http://{ipfs_host}:{ipfs_port}/api/v0/get"
    params = {'arg': ipfs_hash}
    try:
        response = requests.post(url, params=params, timeout=ipfs_timeout)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        logging.error("Error getting from IPFS: %s", str(e))
        raise IPFSException(f"Error getting from IPFS: {e}")

def ipfs_dag_get(ipfs_hash, output_codec='dag-json'):
    """Get a DAG node from IPFS.

    Args:
        ipfs_hash (str): The IPFS hash of the DAG node.
        output_codec (str): Format that the object will be encoded as. Default: 'dag-json'.

    Returns:
        bytes: The contents of the DAG node.

    Raises:
        IPFSException: If an error occurs while getting the DAG node.
    """
    logging.debug("Getting DAG node (or IPFS hash linked content) from IPFS")

    # Read the IPFS configuration from ipfspinner.config file
    config = configparser.ConfigParser()
    config.read("ipfspinner.config")
    ipfs_host = config.get("ipfs", "host")
    ipfs_port = config.getint("ipfs", "port")
    ipfs_timeout = config.getint("ipfs", "timeout")

    url = f"http://{ipfs_host}:{ipfs_port}/api/v0/dag/get"
    params = {
        'arg': ipfs_hash,
        'output-codec': output_codec
    }
    try:
        response = requests.post(url, params=params, timeout=ipfs_timeout)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        logging.error("Error getting DAG node from IPFS: %s", str(e))
        raise IPFSException(f"Error getting DAG node from IPFS: {e}")

def ipfs_pin_add(ipfs_hash):
    """Pin and add an IPFS hash to the local storage.

    Args:
        ipfs_hash (str): The IPFS hash to pin and add.

    Raises:
        IPFSException: If an error occurs while pinning and adding the IPFS hash.
    """
    logging.debug(f"Pinning and adding IPFS hash: {ipfs_hash}")

    # Read the IPFS configuration from ipfspinner.config file
    config = configparser.ConfigParser()
    config.read("ipfspinner.config")
    ipfs_host = config.get("ipfs", "host")
    ipfs_port = config.getint("ipfs", "port")
    ipfs_timeout = config.getint("ipfs", "timeout")

    url = f"http://{ipfs_host}:{ipfs_port}/api/v0/pin/add"
    params = {
        'arg': ipfs_hash,
    }
    try:
        response = requests.post(url, params=params, timeout=ipfs_timeout)
        response.raise_for_status()
        progress_key = 'Progress'
        while progress_key in response.json():
            progress = response.json()[progress_key]
            logging.info(f"Pin progress: {progress}")
            time.sleep(1)
            response = requests.post(url, params=params, timeout=ipfs_timeout)
            response.raise_for_status()
    except requests.exceptions.ReadTimeout as e:
        logging.error("Error pinning IPFS hash: Read timed out.")
        raise IPFSException("Error pinning IPFS hash: Read timed out.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error pinning IPFS hash: {str(e)}")
        raise IPFSException(f"Error pinning IPFS hash: {str(e)}")



def ipfs_repo_stat():
    """Check the status of the IPFS repository.

    Returns:
        bool: True if the repository is available, False otherwise.

    Raises:
        IPFSException: If an error occurs while checking the repository status.
    """
    # Read the IPFS configuration from ipfspinner.config file
    config = configparser.ConfigParser()
    config.read("ipfspinner.config")
    ipfs_host = config.get("ipfs", "host")
    ipfs_port = config.getint("ipfs", "port")
    ipfs_timeout = config.getint("ipfs", "timeout")

    logging.info("Checking IPFS repository status")
    url = f"http://{ipfs_host}:{ipfs_port}/api/v0/id"
    try:
        response = requests.post(url, timeout=ipfs_timeout)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        logging.debug("Error checking IPFS repository status: %s", str(e))
        raise IPFSException(f"Error checking IPFS repository status: {e}")

def ipfs_pin_ls(ipfs_hash, pin_type="recursive", quiet=True, stream=False):
    """List objects pinned to local storage.

    Args:
        ipfs_hash (str): IPFS hash to check. Optional.
        pin_type (str): The type of pinned keys to list. Can be "direct", "indirect", "recursive", or "all". Default: "recursive".
        quiet (bool): Write just hashes of objects. Default: True.
        stream (bool): Enable streaming of pins as they are discovered. Default: False.

    Returns:
        dict: A dictionary containing information about the pinned objects. If the IPFS hash is not pinned, returns None.

    Raises:
        requests.exceptions.RequestException: If an error occurs while listing the pinned objects.
    """
    logging.debug("Listing pinned IPFS objects")

    # Read the IPFS configuration from ipfspinner.config file
    config = configparser.ConfigParser()
    config.read("ipfspinner.config")
    ipfs_host = config.get("ipfs", "host")
    ipfs_port = config.getint("ipfs", "port")
    ipfs_timeout = config.getint("ipfs", "timeout")

    url = f"http://{ipfs_host}:{ipfs_port}/api/v0/pin/ls"
    params = {
        "arg": ipfs_hash,
        "type": pin_type,
        "quiet": quiet,
        "stream": stream
    }
    try:
        ipfs_timeout = int(ipfs_timeout)  # Ensure timeout is an integer
        response = requests.post(url, params=params, timeout=ipfs_timeout)
        response.raise_for_status()
        result = response.json()

        if isinstance(result, dict) and "Message" in result:
            message = result["Message"]
            if message.startswith("path") and "is not pinned" in message:
                logging.info(f"IPFS hash {ipfs_hash} is not pinned.")
                return None

        return result

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 500:
            result = e.response.json()
            if isinstance(result, dict) and "Message" in result:
                message = result["Message"]
                if message.startswith("path") and "is not pinned" in message:
                    logging.info(f"IPFS hash {ipfs_hash} is not pinned.")
                    return None
            logging.warning(f"Error listing pinned IPFS objects: {e}")
            logging.info(f"IPFS hash {ipfs_hash} is not currently pinned. Continuing to retrieve content...")
            return None
        else:
            logging.error(f"Error listing pinned IPFS objects: {e}")
            raise IPFSException(f"Error listing pinned IPFS objects: {e}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error listing pinned IPFS objects: {e}")
        raise IPFSException(f"Error listing pinned IPFS objects: {e}")

#INFO Conversion functions to change characters used in RVN asset names into valid filenames and vice versa. 

#Converts Asset to valid filename
def asset_to_file(asset):
    file = asset
    file = file.replace('/', r'%2F')
    file = file.replace('*', r'%2A')
    file = file.replace('&', r'%26')
    file = file.replace('?', r'%3F')
    file = file.replace(':', r'%3A')
    file = file.replace('=', r'%3D')
    return(file)


#INFO Ravencoin block scanning functions to find assets.

def get_highest_block_height_scanned(dbc):
    """Retrieve the highest block height scanned from the database."""
    dbc.execute('SELECT MAX(block_height) FROM assets')
    result = dbc.fetchone()
    highest_block_height = result[0] if result[0] is not None else 0
    return highest_block_height

def load_block(config, args):
    """
    Find the block to start scanning from.

    Args:
        config (ConfigParser): The configuration object.
        args (argparse.Namespace): The parsed command-line arguments.

    Returns:
        int: The block number to start scanning from.
    """
    block_to_read = config.getint('ipfspinner', 'block')
    logging.info(f'Reading block: {block_to_read}')

    saved_block = None
    try:
        saved_block = config.getint('ipfspinner', 'saved_block', fallback=None)
        logging.debug(f"Retrieved previously scanned block height from config: {saved_block}")
    except (configparser.Error, ValueError):
        pass

    if saved_block is not None and saved_block >= block_to_read:
        calculated_block = max(saved_block, get_highest_block_height_scanned(dbc)) - 120
        logging.info(f"Calculated block based on saved_block and highest block height: {calculated_block}")
    elif args.block is not None and args.block >= block_to_read:
        calculated_block = args.block
        logging.info(f"Using requested arg block height to scan from: {calculated_block}")
    else:
        calculated_block = block_to_read
        logging.info(f"Block height set using config file: {calculated_block}")

    return calculated_block




def save_block(block_num, config):
    """
    Save the 'block_num' to the config file.
    
    Args:
        block_num (int): The block number to save.
        config (ConfigParser): The configuration object.

    Returns:
        None
    """
    try:
        config.set('ipfspinner', 'saved_block', str(block_num))
        with open('ipfspinner.config', 'w') as f:
            config.write(f)
    except IOError:
        logging.error('Failed to save block number to configuration file.')

def scan_asset_blocks(block, folder, config, args):
    """Scan blocks for Ravencoin assets."""
    logger = logging.getLogger(__name__)

    try:
        start_block = load_block(config, args)
        logger.info("Starting at block: %d", start_block)

        blockheight = get_bci().get('blocks')
        end_block = blockheight

        progress = 0

        for i in range(start_block, end_block + 1):
            block_hash = get_blockinfo(i).get('hash')
            logger.info("Scanning block #%d - %s", i, block_hash)
            tx_in_block = get_block(block_hash)
            txs = tx_in_block.get('tx')
            logger.info("Block contains %d transactions", len(txs))

            for tx in txs:
                if paused:
                    logger.info("Scanning paused. Press 's' to resume.")
                    while paused:
                        time.sleep(1)
                        if not paused:
                            logger.info("Resuming scanning...")
                            break

                process_tx(tx, logger, i, config, args)

            save_block(i, config)

            progress = i - start_block + 1
            sys.stdout.write(f"\rProgress: {progress}/{end_block - start_block + 1} ")
            sys.stdout.flush()

        print()

    except Exception as e:
        logger.error("An error occurred while scanning blocks: %s", str(e))

def process_tx(tx, logger, block_num, config, args):
    """Process a transaction and look for Ravencoin assets."""
    logger.debug("Processing transaction %s", tx)
    tx_info = get_rawtx(tx)
    tx_detail = decode_rawtx(tx_info)

    for vout in tx_detail.get('vout'):
        script_asm = vout.get('scriptPubKey').get('asm')
        if script_asm[86:98] == "OP_RVN_ASSET":
            logger.debug("Found OP_RVN_ASSET in transaction %s", tx)
            logger.debug("Asset script: %s", vout.get('scriptPubKey').get('hex'))
            asset_script = decode_script(vout.get('scriptPubKey').get('hex'))
            asset_handler(asset_script, block_num, config, args)

# Monitoring the RVN node for IPFS hashes.
def monitor_zmq():
    logger = logging.getLogger(__name__)
    logger.info("Monitoring Ravencoin transactions...")

    context = zmq.Context()
    socket = context.socket(zmq.SUB)

    logger.info("Getting Ravencoin msgs")
    socket.connect("tcp://localhost:28766")

    socket.setsockopt_string(zmq.SUBSCRIBE, u'hashtx')
    socket.setsockopt_string(zmq.SUBSCRIBE, u'hashblock')
    socket.setsockopt_string(zmq.SUBSCRIBE, u'rawblock')
    socket.setsockopt_string(zmq.SUBSCRIBE, u'rawtx')

    while True:
        msg = socket.recv_multipart()
        topic = msg[0]
        body = msg[1]
        sequence = "Unknown"
        if len(msg[-1]) == 4:
            msgSequence = struct.unpack('<I', msg[-1])[-1]
            sequence = str(msgSequence)
        if topic == b"hashblock":
            logger.info('- HASH BLOCK (%s) -', sequence)
            logger.info(binascii.hexlify(body))
        elif topic == b"hashtx":
            logger.info('- HASH TX  (%s) -', sequence)
            logger.info(binascii.hexlify(body))
        elif topic == b"rawblock":
            logger.info('- RAW BLOCK HEADER (%s) -', sequence)
            logger.info(binascii.hexlify(body[:80]))
        elif topic == b"rawtx":
            logger.info('ZMQ - RAW TX - Sequence: %s', sequence)
            logger.debug('- RAW TX (%s) -', sequence)
            tx_info = binascii.hexlify(body).decode("utf-8")
            logger.debug("txinfo: %s", tx_info)
            tx_detail = decode_rawtx(tx_info)
            for vout in tx_detail.get('vout'):
                if vout.get('scriptPubKey').get('asm')[86:98] == "OP_RVN_ASSET":
                    logger.info("Found OP_RVN_ASSET")
                    logger.info("Asset script: %s", vout.get('scriptPubKey').get('hex'))
                    asset_script = decode_script(vout.get('scriptPubKey').get('hex'))
                    asset_handler(asset_script)


# Main function
def main(argv):
    global dbc, dbconn, ipfs_timeout

    config = setup_config_settings()

    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--noblockscan', action='store_true', help='Do not scan through blocks.')
    parser.add_argument('-z', '--nozmqwatch', action='store_true', help='Do not watch zero message queue.')
    parser.add_argument('-s', '--safemode', action='store_true', help='Only store JSON files of limited size.')
    parser.add_argument('-b', '--block', type=int, help='Start at this block number.', default=config['ipfspinner']['block'])
    parser.add_argument('-f', '--folder', type=str, help='Store files in a different folder.', default=config['ipfspinner']['folder'])
    parser.add_argument('-d', '--debug', action='store_true', help='Print debug info.')
    parser.add_argument('-r', '--noretrieveipfs', action='store_true', help='Do not retrieve IPFS content.')
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    # Get json_only_check value from args and config
    json_only_check = args.safemode

    # Make sure IPFS is running
    try:
        if not ipfs_repo_stat():
            logging.error("Failed to connect to IPFS repository.")
            logging.info("Please make sure IPFS Desktop is running or run 'ipfs daemon' to start the IPFS daemon.")
            exit(-1)
    except Exception as e:
        logging.debug("Error checking IPFS repository status: %s", str(e))
        logging.error("Please make sure IPFS Desktop is running or run 'ipfs daemon' to start the IPFS daemon.")
        exit(-1)

    # Setup the database
    dbconn, dbc = setup_database()

    # Add IPFS peers
    add_ipfs_peers(config)

    # Test if Raven node is running
    try:
        bci = get_bci()
        height = bci['blocks']
        if height is None:
            raise Exception("Error: Raven node not responding with block height.")
    except requests.exceptions.ConnectionError:
        raise Exception("Error: Failed to connect to Raven node")
    except Exception as e:
        logging.debug(f"Error: {e}")
        exit(-1)

    if args.safemode:
        filesize_threshold = 16000
    else:
        filesize_threshold = int(config['ipfspinner']['filesize_threshold'])

    if not args.noblockscan:
        scan_asset_blocks(args.block, args.folder, config, args)

    if not args.noblockscan or (args.noblockscan and not args.nozmqwatch):
        estimate_ipfs_storage_size(filesize_threshold, dbc)

        while True:
            # Check missing assets and attempt to retrieve, add, and pin them
            with dbconn:
                dbc = dbconn.cursor()
                dbc.execute('SELECT asset_name, ipfs_hash FROM assets WHERE pinned = 0 AND added = 0')
                missing_assets = dbc.fetchall()

                for asset_name, ipfs_hash in missing_assets:
                    if not args.noretrieveipfs:
                        retrieve_asset(asset_name, ipfs_hash, args, json_only_check, filesize_threshold, dbc, dbconn)

            if args.noblockscan:
                break

            if not args.nozmqwatch:
                # Monitor ZMQ
                monitor_zmq()


if __name__ == "__main__":
    main(sys.argv[1:])
