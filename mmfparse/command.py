#! /usr/bin/env python

# Author: Michael Falk

import argparse
from mmfparse.core import mmfParser

def run(inputtext, username, password, host, dbname, encoding):
    """Routine for importing data."""
    usr_choice = input("Initialise parser? Please type 'yes' or 'no'...\n")
    if usr_choice == "yes":
        parser = mmfParser(username, password, host, dbname, encoding)
    elif usr_choice =="no":
        print("Nothing was done.")
    else:
        run(inputtext, username, password, host, dbname, encoding)

def main():
    """Parses arguments and applies import script to the raw text file."""

    # Instantiate parser
    parser = argparse.ArgumentParser(description="Convert raw output from the Notebook MMF database into well-formed MySQL.")
    
    # Arguments the user can supply
    parser.add_argument("-i", "--input-text", help="The raw text file you wish to convert.", dest="inputtext")
    parser.add_argument("-e", "--encoding", default="cp1252", help="The encoding of the text file from Notebook. Defaults to windows-1252.")
    parser.add_argument("-db", "--database-name", help="The name of the MySQL database where the output is to be saved.", dest="dbname")
    parser.add_argument("-u", "--username", help="Your username for the database.")
    parser.add_argument("-p", "--password", help="Your password for the database")
    parser.add_argument("-hst", "--host", default="127.0.0.1", help="The IP address of the database. Defaults to localhost.")

    # Parse arguments and convert to dict
    args = vars(parser.parse_args())
    
    # Pass to function
    run(**args)

if __name__=="__main__":
    main()
    
