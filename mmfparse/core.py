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
        '0':'full_identifier',
        '1':'work_identifier',
        '01':'ed_identifier',
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
        '0': 'full_identifier',
        '1': 'work_identifier',
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
        "o+e": "œ",
        "O+E": "Œ",
        "/.../":"[...]",
        "/sic/":"[sic]",
        "/sic==/":"[sic]",
        "/==":"[",
        "==/":"]",
        "/_":"[",
        "_/":"]",
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
            work_identifier CHAR(12),
            translation VARCHAR(128),
            title TEXT,
            comments TEXT,
            bur_references TEXT,
            bur_comments TEXT,
            original_title TEXT,
            translation_comments TEXT,
            description TEXT
        ) ENGINE=InnoDB  CHARSET=utf8mb4
        """,
        # Each 're-edition' is represented by an edition
        'mmf_edition': """
        CREATE TABLE IF NOT EXISTS mmf_edition (
            edition_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            work_id INT,
            uuid CHAR(36) NOT NULL,
            work_identifier CHAR(12),
            ed_identifer CHAR(12),
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
            first_text TEXT
        ) ENGINE=InnoDB  CHARSET=utf8mb4
        """,
        # Each library copy is represented by a holding
        'mmf_holding': """
        CREATE TABLE IF NOT EXISTS mmf_holding (
            holding_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            edition_id INT NOT NULL,
            lib_name VARCHAR(255),
            lib_id INT
        ) ENGINE=InnoDB  CHARSET=utf8mb4
        """,
        # Each library is represented by a library
        'mmf_lib': """
        CREATE TABLE IF NOT EXISTS mmf_lib (
            lib_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            short_name VARCHAR(255),
            full_name TEXT
        ) ENGINE=InnoDB  CHARSET=utf8mb4
        """,
        # Each reference is respresented as a reference
        'mmf_ref': """
        CREATE TABLE IF NOT EXISTS mmf_ref (
            ref_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            work_id INT NOT NULL,
            short_name VARCHAR(255),
            page_num INT,
            ref_work INT,
            ref_type INT NOT NULL
        ) ENGINE=InnoDB  CHARSET=utf8mb4
        """,
        # Each reference is of one of two types
        'mmf_ref_type': """
        CREATE TABLE IF NOT EXISTS mmf_ref_type (
            ref_type_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255)
        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8mb4
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
        ) ENGINE=InnoDB  CHARSET=utf8mb4
        """
    }

    # Combined list of indexes that can be deleted and rebuilt as required
    INDEXES = [
        {
            'table': 'mmf_work',
            'name': 'work_work_identifier',
            'column': 'work_identifier'
        },
        {
            'table': 'mmf_edition',
            'name': 'edition_work_identifier',
            'column': 'work_identifier'
        },
        {
            'table': 'mmf_holding',
            'name': 'holding_edition',
            'column': 'edition_id'
        },
        {
            'table': 'mmf_holding',
            'name': 'holding_lib_name',
            'column': 'lib_name'
        },
        {
            'table': 'mmf_lib',
            'name': 'lib_lib_name',
            'column': 'short_name'
        }
    ]

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
            database=dbname,
            use_unicode = True
            )
        
        # Alert user
        print(f'Connected to database {dbname} on {host} as {username}.')

    def create_tables(self):
        """Creates essential tables for the MMF database."""
        
        # Do any of the tables exist?
        self.cur = self.conn.cursor()
        self.cur.execute('SHOW TABLES')
        table_list = self.cur.fetchall()
        table_list = [tbl for (tbl,) in table_list if tbl in self.SCHEMA.keys()]

        # Inner function for creating table
        def _apply_schema():
            for table, stmt in self.SCHEMA.items():
                # Can't use params because it wraps strings in quotation marks
                self.cur.execute('DROP TABLE IF EXISTS %s' % table)
                self.cur.execute(stmt)
                self.conn.commit()
                print(f'Table {table} created in database {self.dbname}.')
            # Insert values into mmf_ref_type
            self.cur.execute("""
            INSERT INTO mmf_ref_type VALUES
            (NULL,'contemporary'),
            (NULL,'post C18')
            """)
            self.conn.commit()


        # Allow user input, apply the schema
        if len(table_list) > 0:
            print(f'Tables {", ".join(table_list)} already exist.')
            usr_choice = input('Overwrite? y/n\n')
            if usr_choice.startswith('y'):
                confirmation = input('This will overwrite existing tables, are you sure? y/n\n')
                if confirmation.startswith('y'):
                    _apply_schema()
                    self.cur.close()
                else:
                    print('Overwrite not confirmed. Table creation skipped.')
                    self.cur.close()
                    return False
            else:
                print('Table creation skipped.')
                self.cur.close()
                return False
        else:
            _apply_schema()
            self.cur.close()

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
        ds = re.compile(r'\$\s?(?=\n+|$)')
        # Regex for pulling out keys and values
        kv = re.compile(r'<?(\d{1,2}|Incipit)>?:(.+?)\s*(?:\n|$)')
        # Regex for removing null entries
        ne = re.compile(r'^<\d{1,2}>$|^\$?[\s\n]+$')

        # Regex for extracting page numbers from references
        pages_rgx = re.compile(r'\b\d+\b')

        # Regex for checking signature of identifiers
        wrk_id_rgx = re.compile(r'^(?:\d{2}\w{2}|s\.d\.)\.[A-Z ]{3}\.[a-z\d]{3}\b')
        ed_id_rgx = re.compile(r'^.{12} (?:\d{2}\w{2}|s\.d\.)\.[A-Z ]{3}\.[a-z\d]{3}\b')
        
        # Regex for splitting titles
        title_rgx = re.compile(r'Z[12]')

        # Regex for finding deleted or hidden entries
        hidden_rgx = re.compile(r'(\bz+\b|ENTERED|x+(?<!\b\d{2}))', re.I)

        # Regex for extracting library names from holdings
        # It looks for a word consisting of a name (which may contain
        # hyphens), followed by a hyphen and 1-5 capitals
        holding_rgx = re.compile(r'\b[\w\-]+-[A-Z]{1,5}\b')

        # Initialise a cursor and define input sql
        self.cur = self.conn.cursor()
        insert_work = """
        INSERT INTO mmf_work VALUES (NULL,
            %(uuid)s, %(work_identifier)s, %(translation)s,
            %(title)s, %(comments)s, %(bur_references)s, %(bur_comments)s,
            %(original_title)s, %(translation_comments)s,
            %(description)s 
        )
        """
        insert_edition = """
        INSERT INTO mmf_edition VALUES (NULL, %(work_id)s,
            %(uuid)s, %(work_identifier)s, %(ed_identifier)s,
            %(edition_counter)s, %(translation)s,
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
        insert_references = """
        INSERT INTO mmf_ref VALUES (
            NULL, %(work_id)s, %(short_name)s, %(page_num)s, NULL, %(ref_type)s
            )
        """

        # Error logging function
        def _log_error(**kwargs):
            err.update(**kwargs)
            self.cur.execute(insert_error, err)
            self.conn.commit()

        # Drop the indices on the identifier columns
        for idx_dict in self.INDEXES:
            self.cur.execute("DROP INDEX IF EXISTS {name} ON {table}".format(**idx_dict))
        self.conn.commit()

        print("Processing records...")
        err = ErrorDict(inputtext) # Initialise ErrorDict
        successes = 0 # Count successful writes to databse
        errors = 0 # Count errors during import
        pbar = tqdm(total = len(text)) # Initialise progress bar

        # Iterate over the records:
        while len(text) > 0:

            # Get next record
            record = text.pop()
            # Reset error dict
            err.reset()
            unused_codes = set()

            # Extract record into dict
            record = nl.sub(" ", record)  # newlines
            record = ds.sub("", record)  # dollarsigns
            record = dict(kv.findall(record))  # information
            record = {k:v.strip() for k,v in record.items() if not ne.match(v)} # null entries

            # Extract key info from full identifier
            if '0' not in record:
                err.update(
                    text = str(record),
                    error_note = "No identifier")
                self.cur.execute(insert_error, err)
                self.conn.commit()
                errors += 1
                pbar.update(1)
                continue
            
            if hidden_rgx.search(record['0']):
                _log_error(
                    text=str(record),
                    error_note="Hidden or deleted"
                    )
                pbar.update(1)
                continue
            
            if '4' in record and record['4'].startswith('xx'):
                _log_error(
                    text=str(record),
                    error_note="Hidden or deleted"
                )
                pbar.update(1)
                continue
            
            if '04' in record and record['04'].startswith('xx'):
                _log_error(
                    text=str(record),
                    error_note="Hidden or deleted"
                )
                pbar.update(1)
                continue
            
            if '1' not in record and '01' not in record:
                _log_error(
                    text = str(record),
                    error_note = "Incomplete identifiers"
                )
                pbar.update(1)
                continue

            work_identifier = wrk_id_rgx.findall(record['0'])
            ed_identifier = ed_id_rgx.findall(record['0'])

            # Editions have both edition and work identifiers
            if len(ed_identifier) > 0 and len(work_identifier) > 0:
                # Create new dict for the edition
                ed = {}.fromkeys(self.EDITION_CODES.values())

                # Set uuid, edition id, get work identifier
                ed['uuid'] = str(uuid4())
                ed['work_id'] = None # This is unknown for re-editions

                # Loop through the field definitions for editions,
                # and extract the key information
                for code, field in self.EDITION_CODES.items():
                    if code in record:
                        ed[field] = record[code]
                
                # Hoover up any information stored in the wrong fields
                for code, field in self.WORK_CODES.items():
                    if code not in self.EDITION_CODES and field in ed and code in record:
                        if ed[field] is None:
                            ed[field] = record[code]
                        elif ed[field] is not None:
                            unused_codes.add(code)
                
                # If work_identifier has not been provided, include it now
                ed['work_identifier'] = work_identifier[0]
                
                # Add edition identifier
                if ed['ed_identifier'] is None:
                    ed['ed_identifier'] = ed_identifier[0]

                # Delete notes from edition_counter, if there are any
                if ed['edition_counter'] is not None:
                    ed['edition_counter'] = ed['edition_counter'][0:7]

                # Concatenate two halves of title
                if ed['long_title'] is not None and ed['short_title'] is not None:
                    ed['long_title'] = ed['short_title'] + ed['long_title']
                
                # Sometimes the long title is in field 21, with a Z1 or Z2 seperating the two components
                if ed['short_title'] is not None and title_rgx.search(ed['short_title']):
                    segs = title_rgx.split(ed['short_title'])
                    ed['long_title'] = segs[0] + segs[1]
                    ed['short_title'] = segs[0]

                # Insert into DB
                self.cur.execute(insert_edition, ed)
                self.conn.commit()

                # Get new primary key
                self.cur.execute("SELECT LAST_INSERT_ID()")
                edition_id = self.cur.fetchone()[0]

                # Explode holdings and insert them
                if ed['holdings'] is not None:
                    # Extract holdings using regex
                    holdings = holding_rgx.findall(ed['holdings'])
                    # If none are extracted, log an error
                    if len(holdings) == 0:
                        _log_error(
                            text = ed['holdings'],
                            error_note = "Junk holdings",
                            edition_id = edition_id)
                    else:
                        # Create parameter sequence
                        param_seq = [(edition_id, lib) for lib in holdings]
                        # Insert the holdings
                        self.cur.executemany(insert_holdings, param_seq)
                        self.conn.commit()
                
                successes += 1

            # The princeps has no edition identifier
            elif len(work_identifier) > 0:
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
                
                # Hoover up any remaining info
                for code,field in self.EDITION_CODES.items():
                    if code not in self.WORK_CODES and code in record:
                        if field in wk and wk[field] is None:
                            wk[field] = record[code]
                        if field in ed and ed[field] is None:
                            ed[field] = record[code]
                        else:
                            unused_codes.add(code)
                
                # Set uuids
                wk['uuid'] = str(uuid4())
                ed['uuid'] = str(uuid4())

                # Set identifiers and edition counter
                wk['work_identifier'] = ed['work_identifier'] = work_identifier[0]             

                ed['ed_identifier'] = ed['work_identifier']
                ed['edition_counter'] = ed['ed_identifier'][0:4] + '000'
                
                # Unpack title. Work title is short title.
                if wk['title'] is not None:
                    title_parts = title_rgx.split(wk['title'])
                    wk['title'] = title_parts[0]
                    ed['long_title'] = ''.join(title_parts)
                    ed['short_title'] = title_parts[0]
                elif ed['short_title'] is not None:
                    if ed['long_title'] is not None:
                        ed['long_title']

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
                # Extract holdings using regex
                if ed['holdings'] is not None:
                    holdings = holding_rgx.findall(ed['holdings'])
                    # If none are extracted, log an error
                    if len(holdings) == 0:
                        _log_error(
                            text=ed['holdings'],
                            error_note="Junk holdings",
                            edition_id = edition_id
                            )
                    else:
                        # Create parameter sequence
                        param_seq = [(edition_id, lib)
                                     for lib in holdings]
                        # Insert the holdings
                        self.cur.executemany(insert_holdings, param_seq)
                        self.conn.commit()
                
                # Explode references and insert them
                if wk['contemporary_references'] is not None:
                    # Explode into list
                    cr_list = wk['contemporary_references'].split('  ')
                    # Format for insert
                    cr_list = [{'work_id':ed['work_id'], 'short_name':x, 'page_num':None, 'ref_type':1} for x in cr_list]
                    self.cur.executemany(insert_references, cr_list)
                    self.conn.commit()
                
                if wk['later_references'] is not None:
                    # Explode into list
                    lr_list = wk['later_references'].split('  ')
                    # Unpack using pagenum regex 
                    lr_out = []
                    for ref in lr_list:
                        ref_dict = {'work_id':ed['work_id'], 'ref_type':2, 'page_num':None}
                        # Use regex to strip page numbers
                        ref_dict['short_name'] = pages_rgx.sub('', ref).strip()
                        # Extract page numbers...
                        pages = pages_rgx.findall(ref)
                        # If there are any pages, create an entry for each paged reference
                        if len(pages) > 0:
                            for page in pages:
                                # Create copy of dict
                                ref_dict = ref_dict.copy()
                                # Insert new page number
                                ref_dict['page_num'] = page
                                # Append to parameter list for insert query
                                lr_out.append(ref_dict)
                        # Otherwise just add the reference to the out_list
                        else:
                            lr_out.append(ref_dict)
                    
                    self.cur.executemany(insert_references, lr_out)
                    self.conn.commit()

                successes += 1

            else:
                _log_error(
                    text=str(record),
                    error_note="Invalid identifier"
                )
                errors += 1

            if len(unused_codes) > 0:
                _log_error(
                    edition_id=edition_id,
                    text=str(record),
                    error_note=f'Unused codes: {unused_codes}'
                )
                errors += 1

            pbar.update(1)
        
        # Close the progress bar
        pbar.close()

        # Rebuild the indexes
        print('Rebuilding indexes...')
        for idx_dict in self.INDEXES:
            self.cur.execute("CREATE INDEX {name} ON {table} ({column})".format(**idx_dict))
        self.conn.commit()

        # Close the cursor
        self.cur.close()

        print(f'Database update complete. {successes} records inserted, with {errors} errors.')

    def link_books(self):
        """Attempts to link related records across the database.
        
        In principle, every mmf_edition should have an mmf_work corresponding
        to it. The aim of the 'identifier' field in the original database
        was to enable such cross-referencing."""

        # SQL statements
        new_work_stmt = """
        INSERT INTO mmf_work (work_identifier, title, translation)
            SELECT DISTINCT work_identifier, short_title, translation
            FROM mmf_edition
            WHERE mmf_edition.work_identifier NOT IN(SELECT work_identifier FROM mmf_work)
        """
        link_book_stmt = """
        UPDATE mmf_edition AS e
        LEFT JOIN mmf_work AS w ON e.work_identifier = w.work_identifier
        SET e.work_id = w.work_id
        WHERE e.work_id IS NULL
        """

        # Execute statement
        self.cur = self.conn.cursor()
        print("Creating missing works...")
        self.cur.execute(new_work_stmt)
        print(f'{self.cur.rowcount} works created.')
        print("Linking editions to works...")
        self.cur.execute(link_book_stmt)
        print(f'{self.cur.rowcount} links created.')
        self.conn.commit()
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
        print(f'{self.cur.rowcount} new libraries added to mmf_lib.')
        print("Linking holdings to libraries...")
        self.cur.execute(link_lib_stmt)
        print(f'{self.cur.rowcount} links created between libraries and holdings')
        self.conn.commit()
        self.cur.close()

    def deduplicate_books(self):
        """Searches for duplicates in the works table, and merges them."""

        # Create temporary table to work with
        create_dupe_table =  """
        CREATE TEMPORARY TABLE dupe_work
        SELECT * FROM mmf_work
            GROUP BY work_identifier
            HAVING COUNT(work_identifier) > 1
            ORDER BY work_id DESC
        """
        # Update works with missing info from other entries
        # TO DO: Update query
        update_works = """
        UPDATE mmf_work AS w
        LEFT JOIN 
        """

        self.cur = self.conn.cursor()
        print(f'Removing duplicate works...')
        self.cur.execute(create_dupe_table)
        print(f'{self.cur.rowcount} duplicates found.')
        self.cur.execute(update_works)

