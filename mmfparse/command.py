#! /usr/bin/env python

# Author: Michael Falk

import argparse
from mmfparse.core import mmfParser
import pickle as p

def run(inputtext, username, password, host, dbname, encoding, newdb):
    """Routine for importing data."""

    # Initialise parser
    parser = mmfParser(username, password, host, dbname, encoding)

    # Create database
    if newdb:
        parser.create_tables()

    # Import text
    parser.import_records(inputtext)

    # Deduplicate works data
    parser.deduplicate_books()

    # Find links in data
    parser.link_books()

    # Update library data
    parser.update_libraries()


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
    parser.add_argument("-n", "--new-db", action="store_true", help="Set this flag if you want to create a new database or overwrite an existing one.", dest="newdb")

    # Parse arguments and convert to dict
    args = vars(parser.parse_args())
    
    # Pass to function
    run(**args)

if __name__=="__main__":
    main()
    
