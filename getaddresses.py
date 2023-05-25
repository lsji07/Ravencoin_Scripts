# Additonal work required. Script is functional as is. 
# This script is used to obtain new unused public addresses from a local nodes wallet.
# Quantity of addresses decided by the user.

import logging
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

rpc_user = "ravencoinlover"
rpc_password = "kakawww"
rpc_port = 18766  # Update with your Ravencoin RPC port. 8766 for mainnet. 18766 for testnet.
address_file = "addresses.txt"

num_addresses = 1000  # Specify the number of addresses to create

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
#logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")

def create_new_address(rpc_connection):
    try:
        new_address = rpc_connection.getnewaddress()
        return new_address
    except JSONRPCException as e:
        logging.error(f"Error creating new address: {e.error}")
        return None

def save_address_to_file(address):
    try:
        with open(address_file, "a") as f:
            f.write(address + "\n")
    except IOError as e:
        logging.error(f"Error saving address to file: {e}")

def main():
    try:
        rpc_connection = AuthServiceProxy(f"http://{rpc_user}:{rpc_password}@localhost:{rpc_port}")

        progress_interval = num_addresses // 10
        progress_counter = 0

        logging.info(f"Generating {num_addresses} addresses...")

        # Generate the specified number of new addresses and save them to the file
        for i in range(1, num_addresses + 1):
            address = create_new_address(rpc_connection)
            if address:
                save_address_to_file(address)
                progress_counter += 1
                if progress_counter % progress_interval == 0:
                    logging.info(f"Progress: {progress_counter}/{num_addresses} addresses generated")

        logging.info("Address generation completed.")

    except ConnectionRefusedError:
        logging.error("Connection to Ravencoin RPC server refused. Make sure the server is running and accessible.")
    except JSONRPCException as e:
        logging.error(f"RPC connection error: {e}")

if __name__ == "__main__":
    main()

