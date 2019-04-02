import mysql.connector
from uuid import uuid4

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

    # Constant: field codes of the original MMF database
    FIELD_CODES = {
        '0':'identifier_other',
        '1':'identifier',
        '01':'edition_identifier',
        '20':'edition_counter',
        '4':'translation',
        '04':'edition_translation',
        '2':'author',
        '02':'edition_author',
        '3':'translator',
        '03':'edition_translator',
        '5':'title',
        '21':'edition_short_title',
        '22':'edition_long_title',
        '23':'edition_collection_title',
        '6':'publication_details',
        '24':'edition_publication_details',
        '7':'location',
        '8':'contemporary_references',
        '9':'later_references',
        '25':'edition_location',
        '10':'comments',
        '26':'edition_comments',
        '11':'references_BUR',
        '12':'comments_BUR',
        '13':'original_title',
        '14':'translation_comments',
        '15':'description',
        '16':'princeps_entry',
        '27':'end_re_edition_entry',
        '18':'re_editions',
        '19':'final_comments',
        '17':'end_full_entry',
        '30':'first_text',
        'Incipit':'first_text'
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
        "\"":"'",
        ". .":".."
    }

    # Constant: sql schema of MMF database
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
            work_id INT NOT NULL,
            uuid CHAR(36) NOT NULL,
            identifier VARCHAR(255),
            translation VARCHAR(128),
            author VARCHAR(255),
            translator VARCHAR(255),
            short_title VARCHAR(255),
            long_title TEXT,
            collection_title TEXT,
            publication_details TEXT,
            comments TEXT,
            INDEX(identifier)
        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8
        """,
        # Each library copy is represented by a holding
        'mmf_holding': """
        CREATE TABLE IF NOT EXISTS mmf_holding (
            holding_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            edition_id INT NOT NULL,
            lib_id INT NOT NULL
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
                    print('Overwrite not confrimed. Table creation skipped.')
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

    def import_text(self):
        """Imports text from a Notebook output file into the MMF database."""

        


        return None
