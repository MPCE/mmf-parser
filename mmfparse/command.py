#! /usr/bin/env python

# Author: Michael Falk

import argparse

def run(args):
    print(args.inputtext)

def main():
    """Parses arguments and applies import script to the raw text file."""

    # Instantiate parser
    parser = argparse.ArgumentParser(description="Convert raw output from the Notebook MMF database into well-formed MySQL.")
    
    # Arguments the user can supply
    parser.add_argument("-i", "--inputtext", help="The raw text file you wish to convert.")
    parser.add_argument("-db", "--databasename", help="The name of the MySQL database where the output is to be saved.")
    parser.add_argument("-u", "--username", help="Your username for the database.")
    parser.add_argument("-p", "--password", help="Your password for the database")

    # Define what to do with the arguments once they are parsed
    parser.set_defaults(func=run)

    # Parse arguments and pass to processing function
    args = parser.parse_args()
    args.func(args)

if __name__=="__main__":
    main()
    