import mysql.connector
from uuid import uuid4
import re
from tqdm import tqdm
from datetime import date

from .util import DupeDict, ErrorDict

class mmfParser(object):
    """Main class for parsing MMF output files.
    
    This object connects to the MMF database, either locally or on a remote server,
    and provides methods for importing and properly formatting data from
    printouts provided by the old MMF Notebook database.

    Parameters:
    -----------
    username (str): username for MySQL database
    password (str): password for MySQL database
    host (str): IP address of database
    dbname (str): the name of the database
    encoding (str): the encoding of the input MMF text file
    """

    # Constants: field codes of the original MMF database
    EDITION_CODES = {
        '01':'identifier',
        '011':'edition_counter', # This is complicated. See below how the DupeDict is used
        '20':'edition_counter',
        '04':'translation',
        '02':'author',
        '03':'translator',
        '21':'short_title', # Actually the start of the title
        '22':'long_title', # Actually the rest of the title
        '23':'collection_title',
        '24':'publication_details',
        '25':'holdings',
        '26':'comments',
        '19':'final_comments',
        '30':'first_text',
        'Incipit':'first_text'
    }
    WORK_CODES = {
        '01': 'identifier',
        '4': 'translation',
        '2': 'author',
        '3': 'translator',
        '5': 'title',
        '6': 'publication_details',
        '7': 'holdings',
        '8': 'contemporary_references',
        '9': 'later_references',
        '10': 'comments',
        '11': 'bur_references',
        '12': 'bur_comments',
        '13': 'original_title',
        '14': 'translation_comments',
        '15': 'description',
        '18': 're_editions',
        '19': 'final_comments',
        '30': 'first_text',
        'Incipit': 'first_text'
    }

    # Constant: ASCII codes from original MMF markup
    ASCII_CODES = {
        "E%":"É",
        "{":"é",
        "}":"è",
        "@":"à",
        "e^":"ê",
        "a^":"â",
        "i^":"î",
        "o^":"ô",
        "u^":"û",
        "a~":"ä",
        "e~":"ë",
        "i~":"ï",
        "o~":"ö",
        "u~":"ü",
        "\\": "ç",  # One backslash represents a ç
        "O|":"Où",
        "o|":"où",
        "o+e":"œ",
        "O+E":"Œ",
        "/.../":"[...]",
        "/sic/":"[sic]",
        "/sic==/":"[sic]",
        "/==":"[",
        "==/":"]",
        "/_":"[",
        "_/":"]",
        "Z2":"",
        "Z3":"",
        "`":"'",
        "\"":"'"
    }

    # Constant: sql schema of new MMF database
    SCHEMA = {
        # Each 'princeps' is represented by a work
        'mmf_work': """
        CREATE TABLE IF NOT EXISTS mmf_work (
            work_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            uuid CHAR(36) NOT NULL,
            identifier VARCHAR(255),
            translation VARCHAR(128),
            title TEXT,
            comments TEXT,
            bur_references TEXT,
            bur_comments TEXT,
            original_title TEXT,
            translation_comments TEXT,
            description TEXT,
            INDEX(identifier)
        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8
        """,
        # Each 're-edition' is represented by an edition
        'mmf_edition': """
        CREATE TABLE IF NOT EXISTS mmf_edition (
            edition_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            work_id INT,
            uuid CHAR(36) NOT NULL,
            identifier VARCHAR(255),
            edition_counter CHAR(7),
            translation VARCHAR(128),
            author VARCHAR(255),
            translator VARCHAR(255),
            short_title VARCHAR(255),
            long_title TEXT,
            collection_title TEXT,
            publication_details TEXT,
            comments TEXT,
            final_comments TEXT,
            first_text TEXT,
            INDEX(identifier)
        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8
        """,
        # Each library copy is represented by a holding
        'mmf_holding': """
        CREATE TABLE IF NOT EXISTS mmf_holding (
            holding_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            edition_id INT NOT NULL,
            lib_name VARCHAR(255),
            lib_id INT,
            INDEX(edition_id),
            INDEX(lib_id)
        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8
        """,
        # Each library is represented by a library
        'mmf_lib': """
        CREATE TABLE IF NOT EXISTS mmf_lib (
            lib_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            short_name VARCHAR(255),
            full_name TEXT
        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8
        """,
        # Each reference is respresented as a reference
        'mmf_ref': """
        CREATE TABLE IF NOT EXISTS mmf_ref (
            ref_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            short_name VARCHAR(255),
            bibliographic_reference TEXT,
            ref_type INT NOT NULL,
            page_num INT
        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8
        """,
        # Each reference is of one of two types
        'mmf_ref_type': """
        CREATE TABLE IF NOT EXISTS mmf_ref_type (
            ref_type_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255)
        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8
        """,
        # Table for recording errors
        'mmf_error': """
        CREATE TABLE IF NOT EXISTS mmf_error (
            error_id INT AUTO_INCREMENT PRIMARY KEY,
            filename VARCHAR(255),
            edition_id INT,
            work_id INT,
            text TEXT,
            error_note VARCHAR(255),
            date DATE
        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8
        """
    }

    def __init__(self, username, password, host, dbname, encoding):
        
        # Store arguments
        self.username = username
        self.password = password
        self.host = host
        self.dbname = dbname
        self.encoding = encoding

        # Connect to MySQL database
        self.conn = mysql.connector.connect(
            user=username,
            password=password,
            host=host,
            database=dbname
            )
        
        # Alert user
        print(f'Connected to database {dbname} on {host} as {username}.')

    def create_tables(self):
        """Creates essential tables for the MMF database."""
        
        # Do any of the tables exist?
        cur = self.conn.cursor()
        cur.execute('SHOW TABLES')
        table_list = cur.fetchall()
        table_list = [tbl for (tbl,) in table_list if tbl in self.SCHEMA.keys()]

        # Inner function for creating table
        def _apply_schema():
            for table, stmt in self.SCHEMA.items():
                cur.execute('DROP TABLE IF EXISTS %s' % table) # Can't use params because it wraps strings in quotation marks
                cur.execute(stmt)
                self.conn.commit()
                print(f'Table {table} created in database {self.dbname}.')

        # Allow user input, apply the schema
        if len(table_list) > 0:
            print(f'Tables {", ".join(table_list)} already exist.')
            usr_choice = input('Overwrite? y/n\n')
            if usr_choice.startswith('y'):
                confirmation = input('This will overwrite existing tables, are you sure? y/n\n')
                if confirmation.startswith('y'):
                    _apply_schema()
                    cur.close()
                else:
                    print('Overwrite not confirmed. Table creation skipped.')
                    cur.close()
                    return False
            else:
                print('Table creation skipped.')
                cur.close()
                return False
        else:
            _apply_schema()
            cur.close()

        return True

    def import_records(self, inputtext):
        """Imports records from a Notebook output file into the MMF database."""

        # Read the text file into memory
        with open(inputtext, 'r', encoding=self.encoding, errors='ignore') as f:
            # There are some non-ASCII characters that have crept in to notes
            # copied and pasted from the web, so errors must be set to 'ignore'
            text = f.read()

        # Process using ASCII table
        for old, new in self.ASCII_CODES.items():
            text = text.replace(old, new)
        
        # Split
        text = text.split("\n%End:\n")
        
        # Define regexes for processing each record as a string.
        # Each regex relies on the prior application of the one before.

        # Regex for stripping out extraneous newlines
        nl = re.compile(r'\n(?!%)')
        # Regex for stripping out extraneous dollar signs
        ds = re.compile(r'\$(?=\n|$)')
        # Regex for pulling out keys and values
        kv = re.compile(r'<?(\d{1,2}|Incipit)>?:(.+?)\s*(?:\n|$)')
        # Regex for removing null entries
        ne = re.compile(r'^<\d{1,2}>$|^\s+$')

        # Now iterate over the list and extract key information

        # Initialise a cursor and define input sql
        self.cur = self.conn.cursor()
        insert_work = """
        INSERT INTO mmf_work VALUES (NULL,
            %(uuid)s, %(identifier)s, %(translation)s,
            %(title)s, %(comments)s, %(bur_references)s, %(bur_comments)s,
            %(original_title)s, %(translation_comments)s,
            %(description)s 
        )
        """
        insert_edition = """
        INSERT INTO mmf_edition VALUES (NULL, %(work_id)s,
            %(uuid)s, %(identifier)s, %(edition_counter)s, %(translation)s,
            %(author)s, %(translator)s, %(short_title)s, %(long_title)s,
            %(collection_title)s, %(publication_details)s,
            %(comments)s, %(final_comments)s, %(first_text)s
        )
        """
        insert_holdings = """
        INSERT INTO mmf_holding VALUES (NULL, %s, %s, NULL)
        """
        insert_error = """
        INSERT INTO mmf_error VALUES (
            NULL, %(filename)s, %(edition_id)s,
            %(work_id)s, %(text)s, %(error_note)s, %(date)s
            )
        """

        print("Processing records...")
        err = ErrorDict(inputtext) # Initialise ErrorDict
        successes = 0
        errors = 0
        for record in tqdm(text):

            err.reset()

            # Extract record into dict
            record = nl.sub(" ", record)  # newlines
            record = ds.sub("", record)  # dollarsigns
            record = DupeDict(kv.findall(record))  # information
            record = {k:v for k,v in record.items() if not ne.match(v)} # null entries

            # Insert into database

            # Is this a princeps or a re-edition?
            # Editions are distinguished by their 'edition_counter'
            if '01' and '011' in record or '01' and '20' in record:
                # Create new dict for the edition
                ed = {}.fromkeys(self.EDITION_CODES.values())

                # Set uuid and edition id
                ed['uuid'] = str(uuid4())
                ed['work_id'] = None # This is unknown for re-editions

                # Loop through the field definitions for editions,
                # and extract the key information
                for code, field in self.EDITION_CODES.items():
                    if code in record:
                        ed[field] = record[code]

                # Concatenate two halves of title
                if ed['long_title'] is not None:
                    ed['long_title'] = ed['short_title'] + ed['long_title']

                # Insert into DB
                self.cur.execute(insert_edition, ed)
                self.conn.commit()

                # Get new primary key
                self.cur.execute("SELECT LAST_INSERT_ID()")
                edition_id = self.cur.fetchone()[0]

                # Explode holdings and insert them
                if ed['holdings'] is not None:
                    # Explode the list of holdings
                    holdings = ed['holdings'].split(',')
                    # Check to see if split worked, if not, split on two spaces:
                    if len(holdings) == 1:
                        holdings = holdings[0].split('  ')
                    # Create sequence of params for insert
                    # Trim any whitespace from the library names as we go
                    param_seq = [(edition_id, lib.strip()) for lib in holdings]
                    # Insert them
                    self.cur.executemany(insert_holdings, param_seq)
                    self.conn.commit()
                
                successes += 1

                # Check all data has been imported
                unused_codes = set(record) - set(self.EDITION_CODES)
                if len(unused_codes) > 0:
                    err.update(
                        edition_id=edition_id,
                        text=str(record),
                        error_note=f'Unused codes: {unused_codes}'
                    )
                    self.cur.execute(insert_error, err)
                    self.conn.commit()
                    errors += 1


            # Titles of works are stored in field five
            elif '5' in record and len(record['5']) > 0:
                # Works in MMF2 need to be split into works and editions
                wk = {}.fromkeys(self.WORK_CODES.values())
                ed = {}.fromkeys(self.EDITION_CODES.values())

                # Extract data:
                for code, field in self.WORK_CODES.items():
                    if code in record:
                        wk[field] = record[code]
                
                # Copy relevant data to edition dict:
                for k, v in wk.items():
                    if k in ed:
                        ed[k] = v

                # Set uuids
                wk['uuid'] = str(uuid4())
                ed['uuid'] = str(uuid4())

                # Set edition counter
                if ed['identifier'] is not None:
                    ed['edition_counter'] = ed['identifier'][0:4] + '000'
                
                # Unpack title. Work title is short title.
                title_parts = wk['title'].split('Z1')
                wk['title'] = title_parts[0]
                ed['long_title'] = ''.join(title_parts)
                ed['short_title'] = title_parts[0]

                # Insert work
                self.cur.execute(insert_work, wk)
                self.conn.commit()
                
                # Get primary key
                self.cur.execute('SELECT LAST_INSERT_ID()')
                ed['work_id'] = self.cur.fetchone()[0]

                # Insert edition
                self.cur.execute(insert_edition, ed)
                self.conn.commit()

                # Get new primary key
                self.cur.execute('SELECT LAST_INSERT_ID()')
                edition_id = self.cur.fetchone()[0]

                # Explode holdings and insert them
                # NB: above, holdings are seperated by commas, here by spaces
                if ed['holdings'] is not None:
                    # Explode the list of holdings
                    holdings = ed['holdings'].split('  ')
                    # Check to see if split worked, if not, split on commas:
                    if len(holdings) == 1:
                        holdings = holdings[0].split(',')
                    # Create sequence of params for insert
                    # Trim any whitespace from the library names as we go
                    param_seq = [(edition_id, lib.strip()) for lib in holdings]
                    # Insert them
                    self.cur.executemany(insert_holdings, param_seq)
                    self.conn.commit()

                successes += 1

                # Check all data has been imported
                unused_codes = set(record) - set(self.WORK_CODES)
                if len(unused_codes) > 0:
                    err.update(
                        work_id=ed['work_id'],
                        text=str(record),
                        error_note=f'Unused codes: {unused_codes}'
                    )
                    self.cur.execute(insert_error, err)
                    self.conn.commit()
                    errors += 1

            else:
                err.update(
                    text=str(record),
                    error_note="Neither princeps nor re-edition"
                )
                self.cur.execute(insert_error, err)
                self.conn.commit()
                errors += 1

        # Close the cursor
        self.cur.close()

        print(f'Database update complete. {successes} records inserted, with {errors} errors.')

        return None

    def link_books(self):
        """Attempts to link related records across the database.
        
        In principle, every mmf_edition should have an mmf_work corresponding
        to it. The aim of the 'identifier' field in the original database
        was to enable such cross-referencing."""

        # SQL statement
        link_book_stmt = """
        UPDATE mmf_edition AS e
        LEFT JOIN mmf_work AS w ON e.identifier = w.identifier
        SET e.work_id = w.work_id
        WHERE e.work_id IS NULL
        """

        # Execute statement
        self.cur = self.conn.cursor()
        print("Linking editions to works...")
        self.cur.execute(link_book_stmt)
        num_affected = self.cur.rowcount
        self.conn.commit()
        print(f'{num_affected} links created.')
        self.cur.close()
    
    def update_libraries(self):
        """Updates library table based on holdings."""

        # SQL to create new libraries
        new_lib_stmt = """
        INSERT INTO mmf_lib (short_name)
            SELECT DISTINCT mmf_holding.lib_name
            FROM mmf_holding
            WHERE mmf_holding.lib_name NOT IN(SELECT short_name FROM mmf_lib) 
        """
        # Update links to mmf_holding
        link_lib_stmt = """
        UPDATE mmf_holding AS h
        LEFT JOIN mmf_lib AS l ON h.lib_name = l.short_name
        SET h.lib_id = l.lib_id
        WHERE h.lib_id IS NULL
        """

        # Execute
        self.cur = self.conn.cursor()
        print("Updating library table...")
        self.cur.execute(new_lib_stmt)
        new_libs = self.cur.rowcount
        print("Linking holdings to libraries...")
        self.cur.execute(link_lib_stmt)
        new_links = self.cur.rowcount
        self.conn.commit()
        print(f'{new_libs} new libraries added to mmf_lib.')
        print(f'{new_links} links created between libraries and holdings')
        self.cur.close()

    
