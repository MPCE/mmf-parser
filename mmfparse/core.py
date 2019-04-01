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
        '0':'indenfitifer_other',
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
        "\\":"ç",
        "O|":"Où",
        "o|":"où",
        "o+e":"œ",
        "O+E":"Œ",
        r"/.../":"[...]",
        r"/sic/":"[sic]",
        r"/sic==/":"[sic]",
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

    def __init__(self, username, password, host, dbname, encoding):
        
        # Set encoding of input text file
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

        # SQL for table creation:
        work_tbl_stmt = """
        CREATE TABLE IF NOT EXISTS mmf_work (
            id INT NOT NULL AUTO_INCREMENT,
            uuid CHAR(36) NOT NULL,
            identifier VARCHAR(255),
            translation VARCHAR(128),

            PRIMARY KEY(id)
        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8;
        """

        ed_tbl_stmt = """
        CREATE TABLE IF NOT EXISTS mmf_edition (
            id INT NOT NULL AUTO_INCREMENT,
            uuid CHAR(36) NOT NULL,
            identifier VARCHAR(255),
            translation VARCHAR(128)
            PRIMARY KEY(id)
        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8;
        """
        
        # Create tables if they do not exist
        self.cur = self.conn.cursor()
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS `mmf_work` (
            `ID` int(25) NOT NULL AUTO_INCREMENT,
            `Identifier_Other` text,
            `Identifier` text,
            `Identifier_Edition` text,
            `Edition_Counter` text,
            `Translation` text,
            `Translation_Edition` text,
            `Author` text,
            `Author_Edition` text,
            `Translator` text,
            `Translator_Edition` text,
            `Title` text,
            `Edition_Short_Title` text,
            `Edition_Long_Title` text,
            `Edition_Collection_Title` text,
            `Publication_Details` text,
            `Edition_Publication_Details` text,
            `Location` text,
            `References_Contemporary` text,
            `References_Post_18C` text,
            `Edition_Location` text,
            `Comments` text,
            `Edition_Comments` text,
            `References_BUR` text,
            `Comments_BUR` text,
            `Original_Title` text,
            `Comments_Translation` text,
            `Description` text,
            `Princeps_Entry` text,
            `End_Re_Edition_Entry` text,
            `Re_Editions` text,
            `Final_Comments` text,
            `End_Full_Entry` text,
            `First_Text` text,
            `UUID` text,
            `FBTEE_Author_Code` text,
            PRIMARY KEY (`ID`)
        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8;
        """)
        print('Table mmf_raw created in database.')
        return None

    def import_text(self):
        """Imports text from a Notebook output file into the MMF database."""



        return None
