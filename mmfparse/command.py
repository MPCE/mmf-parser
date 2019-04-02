#! /usr/bin/env python

# Author: Michael Falk

import argparse
from mmfparse.core import mmfParser

def run(inputtext, username, password, host, dbname, encoding):
    """Routine for importing data."""

    # Initialise parser
    parser = mmfParser(username, password, host, dbname, encoding)

    # Create database
    parser.create_tables()

    # Import text
    parser.import_text(inputtext)


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
    
