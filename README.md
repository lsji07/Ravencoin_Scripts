# Ravencoin_Scripts
## Scripts to run alongside Ravencoin node.


### Number 1 - rewards.py

  The rewards script is to help people generally easily issue rewards to their asset holders on Ravencoin.

  The script takes some user input/setup initially and can help you get familiar with Ravencoin and RPC connections.

  This script should work on UNIX and windows systems. I specifically avoided using imports that are not cross system compatible.

  The intent is to assist issuers of assets to more easily issue Ravencoin assets as rewards to existing holders of tokens.

  Install the required depends using the below. On windows systems ensure python is installed. Tested using version 3.10. 

      pip install bitcoinrpc
      pip install sqlite3


### Number 2 - ipfs_pinner.py beta

 The script takes some user input and setup and is intended to help most computer minded people setup the node local communications over RPC. 

 When running the script, it will ask the user questions and setup a config file specific to that machine and user in the folder the script is run in.

 This script should work on UNIX and windows systems. I specifically avoided using imports that are not cross compatible.

 The intent is to assist users to pin IPFS content directly from their Ravencoin node to their local IPFS node.

 Install the required depends using the below. On windows systems ensure python is installed.

 Tested using Python version 3.10. 

 Ensure the below dependancies are available.

    pip install bitcoinrpc
    pip install sqlite3
    
## Improvements


This is free open source software so raise issues, make requests, fork it and and make your own improvements and I'll be happy to check it out and add sensible things that add value to the community. I know I am not perfect so let me know about any mistakes you see and we will address them as constructively as possible.
