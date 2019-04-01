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

    def create_tables(self):
        """Creates essential tables for the MMF database."""
        
        # Create tables if they do not exist
        self.cur = self.conn.cursor()
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS `mmf_raw` (
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
