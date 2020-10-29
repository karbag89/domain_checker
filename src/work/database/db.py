import psycopg2


class DB:
    """
    Initialize the PostgreSQL DB connection and create appropriate
    table with appropriate columns. Writing data to table.

    """
    def __init__(self, db_name, db_user, db_pass, db_host, db_port=5432):
        self.db_name = db_name
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_host = db_host
        self.db_port = db_port

    def initilizeTable(self, tbl_name="Domains"):
        """
        Initialize the PostgreSQL DB connection and create appropriate
        table with appropriate columns.
        """
        try:
            conn = psycopg2.connect(database=self.db_name,
                                    user=self.db_user,
                                    password=self.db_pass,
                                    host=self.db_host,
                                    port=self.db_port)
        except Exception as err:
            print("Database not connected. Please check PostgreSQL \
                  connection credentials.", err)
        cur = conn.cursor()
        try:
            cur.execute("""DROP TABLE {}""".format(tbl_name))
            conn.commit()
            cur.execute("""
            CREATE TABLE {}
            (ID INT PRIMARY KEY NOT NULL,
            DOMAIN TEXT,
            DOAMIN_STATUS BOOLEAN,
            EXPIRATION_DATE timestamp,
            CREATION_DATE timestamp,
            COUNTRY TEXT,
            NAME TEXT,
            ORG TEXT,
            ADDRESS TEXT,
            CITY TEXT,
            STATE TEXT,
            ZIPCODE TEXT
            )
            """.format(tbl_name))
        except psycopg2.errors.UndefinedTable as err:
            print("For fixing an error, please read error information.", err)
            conn.rollback()
            cur.execute("""
            CREATE TABLE {}
            (ID INT PRIMARY KEY NOT NULL,
            DOMAIN TEXT,
            DOAMIN_STATUS BOOLEAN,
            EXPIRATION_DATE timestamp,
            CREATION_DATE timestamp,
            COUNTRY TEXT,
            NAME TEXT,
            ORG TEXT,
            ADDRESS TEXT,
            CITY TEXT,
            STATE TEXT,
            ZIPCODE TEXT
            )
            """.format(tbl_name))
        conn.commit()
        conn.close()

    def writeDataToTabel(self, data, tbl_name="Domains"):
        """
        Writing data to table.
        """
        try:
            conn = psycopg2.connect(database=self.db_name,
                                    user=self.db_user,
                                    password=self.db_pass,
                                    host=self.db_host,
                                    port=self.db_port)
        except Exception as err:
            print("Database not connected. Please check PostgreSQL \
                  connection credentials.", err)
        cur = conn.cursor()
        cur.execute(f"""INSERT INTO {tbl_name} (ID, DOMAIN, DOAMIN_STATUS, \
                                                EXPIRATION_DATE, \
                                                CREATION_DATE, COUNTRY, \
                                                NAME, ORG, ADDRESS, CITY, \
                                                STATE, ZIPCODE) \
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                               %s)""", (data[0], data[1], data[2], data[3],
                                        data[4], data[5], data[6], data[7],
                                        data[8], data[9], data[10], data[11]))
        conn.commit()
        conn.close()
