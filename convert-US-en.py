from datetime import datetime
import sys
import pyodbc
import cx_Oracle
import keyring

# Characters not allowed at the beginning of table names
caracNaoPermitidos = ["$","#","_"]
# Indicates whether existing tables in the source should be recreated
recriar=False

# Enter the connection data in the strings below
sql_server_connection_string = keyring.get_password("sqlserver", "connectionstring")
oracle_connection_string = keyring.get_password("sqloracle", "connectionstring")

sql_server_connection = pyodbc.connect(sql_server_connection_string)
oracle_connection = cx_Oracle.connect(oracle_connection_string)

sql_server_cursor = sql_server_connection.cursor()

# Retrieve all tables from the source database (SQL Server) in alphabetical order
sql_server_cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME")
tables = sql_server_cursor.fetchall()

# Process each table
for table in tables:
    table_name = table[0]
    nomeDeTabelaRuim = False
    try:
        nomeDeTabelaRuim = caracNaoPermitidos.index(table_name[:1]) >= 0
    except ValueError:
        nomeDeTabelaRuim = False

    if nomeDeTabelaRuim == False:
        print(f"Processing table {table_name.upper()}...", end='')

        # Check if the table already exists in the destination database
        oracle_cursor = oracle_connection.cursor()
        oracle_cursor.execute(f"SELECT count(*) FROM user_tables WHERE table_name = '{table_name.upper()}'")
        table_exists = oracle_cursor.fetchone()[0] > 0
        oracle_cursor.close()

        # Check if the table already exists and if it needs to be recreated
        if table_exists and recriar :
            # Select all constraints from the destination table and disable them to avoid problems when dropping the table
            oracle_cursor = oracle_connection.cursor()
            oracle_cursor.execute(f"SELECT constraint_name FROM all_constraints WHERE table_name = '{table_name.upper()}' AND constraint_type = 'R'")
            for constraint_name in oracle_cursor:
                try:
                    oracle_cursor.execute(f"ALTER TABLE {table_name} DISABLE CONSTRAINT {constraint_name[0]}")
                except:
                    print(f"Error disabling constraint {constraint_name[0]}")
                    quit()

            oracle_cursor.close()

            # Drop the table in the destination database
            print(f" Dropping existing table {table_name.upper()} => DROP TABLE {table_name}...", end='')
            oracle_cursor = oracle_connection.cursor()
            try:
                oracle_cursor.execute(f"DROP TABLE {table_name}")
            except:
                print(f"Error dropping table")
                quit()

            oracle_cursor.close()
            oracle_connection.commit()
        else:
            if table_exists:
                print(f" Skipping, table already exists.", end='')

        # Check if we should proceed with creating the table in the destination database and inserting data from the source table
        if table_exists == False or recriar:
            print("")

            # Select columns, types, sizes, whether it allows null, whether it is a primary key, etc. to build the CREATE TABLE command for the destination
            sqlTableSchema = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, COLUMN_DEFAULT, COLS.IS_NULLABLE, is_identity, ISNULL(PK.PRIMARYKEY, 0) AS PRIMARYKEY "
            sqlTableSchema = sqlTableSchema + "FROM INFORMATION_SCHEMA.COLUMNS COLS JOIN SYS.columns C ON C.object_id = object_id(COLS.TABLE_NAME) AND C.name = COLUMN_NAME "
            sqlTableSchema = sqlTableSchema + "LEFT JOIN (SELECT table_name AS TABELA, column_name AS COLUNA, 1 AS PRIMARYKEY FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
            sqlTableSchema = sqlTableSchema + "WHERE OBJECTPROPERTY(OBJECT_ID(constraint_name), 'IsPrimaryKey') = 1) PK ON PK.TABELA=COLS.TABLE_NAME AND PK.COLUNA=COLS.COLUMN_NAME "
            sqlTableSchema = sqlTableSchema + f"WHERE TABLE_NAME = '{table_name}' ORDER BY ORDINAL_POSITION "

            sql_server_cursor.execute(sqlTableSchema)
            columns = sql_server_cursor.fetchall()

            create_table_query = f"CREATE TABLE {table_name} ("

            cols = []
            for column in columns:
                cols.append(column)

                column_name = column[0]
                data_type = column[1].lower()
                data_size = column[2]
                value_default = column[3]
                is_nullable = column[4]
                is_identity = bool(column[5])
                is_primarykey = int(column[6])

                if data_type == "datetime" and isinstance(value_default, str):
                    value_default = value_default.replace("(","").replace(")","")
                    if (len(value_default) > 10):
                        value_default = f"TO_DATE({value_default},'YYYY-MM-DD HH24:MI:SS')"
                    else:
                        value_default = f"TO_DATE({value_default},'YYYY-MM-DD')"

                complement = ""
                if is_identity :
                    complement = complement + " GENERATED BY DEFAULT ON NULL AS IDENTITY"
                if is_primarykey == 1 :
                    complement = complement + " PRIMARY KEY"
                if(value_default != None):
                    complement = complement + f" DEFAULT {value_default}"
                if is_nullable == 'NO' :
                    complement = complement + " NOT NULL"

                # Map SQL Server (source) data types to Oracle (destination) data types, using the most appropriate equivalents
                match data_type:
                    case "image":
                        create_table_query += f"{column_name} long raw{complement},"
                    case "varbinary":
                        create_table_query += f"{column_name} long raw{complement},"
                    case "bit":
                        create_table_query += f"{column_name} number(1){complement},"
                    case "varchar":
                        if (data_size>0 and data_size<=2000):
                            create_table_query += f"{column_name} nvarchar2({data_size}){complement},"
                        else:
                            create_table_query += f"{column_name} nclob{complement},"
                    case "nvarchar":
                        if (data_size>0 and data_size<=2000):
                            create_table_query += f"{column_name} nvarchar2({data_size}){complement},"
                        else:
                            create_table_query += f"{column_name} nclob{complement},"
                    case "char":
                        create_table_query += f"{column_name} char({data_size}){complement},"
                    case "datetime":
                        create_table_query += f"{column_name} date{complement},"
                    case "smalldatetime":
                        create_table_query += f"{column_name} date{complement},"
                    case "bigint":
                        create_table_query += f"{column_name} number(20){complement},"
                    case "text":
                        create_table_query += f"{column_name} nclob{complement},"
                    case "money":
                        create_table_query += f"{column_name} number(19,4){complement},"
                    case _:
                        create_table_query += f"{column_name} {data_type}{complement},"

            create_table_query = create_table_query.rstrip(',') + ")"

            oracle_cursor = oracle_connection.cursor()
            try:
                # Execute the CREATE TABLE command in the destination
                oracle_cursor.execute(create_table_query)
            except:
                print(f"create_table_query = {create_table_query}")
                oracle_connection.rollback()
                oracle_cursor.close()
                oracle_connection.close()
                sql_server_cursor.close()
                sql_server_connection.close()
                quit()

            oracle_connection.commit()

            # Create the reader for the source table data
            sql_server_cursor.execute(f"SELECT * FROM {table_name}")
            rows = sql_server_cursor.fetchall()

            for row in rows:

                # Start the INSERT command to insert the data into the destination table
                insert_query = f"INSERT INTO {table_name} ("
                for col in cols:
                    insert_query += col[0] + ","
                
                insert_query = insert_query.rstrip(',') + ") VALUES ("

                i=-1
                datas = []
                bytesToCommit = 0
                for value in row:
                    i += 1
                    is_nullable = cols[i][4]
                    length = 0
                    if isinstance(value, str): 
                        length = len(value)

                    # print(f"{cols[i][0]} - type(value): {type(value)} - is_nullable: {is_nullable} - valor: {value} - length: {length}")
                    insert_query += f":{cols[i][0]},"

                    # Handle some data to avoid errors when the column does not allow NULL.
                    # For example, in SQL Server a varchar column that does not allow NULL accepts the content "",
                    # while Oracle does not allow it and requires a whitespace " ".
                    if is_nullable == 'NO' :
                        if value == None:
                            if isinstance(value, (int, float)):
                                value = 0
                            if isinstance(value, str):
                                value = " "
                            if isinstance(value, str):
                                value = " "
                        elif isinstance(value, str):
                            if len(value) == 0:
                                value = " "

                    
                    datas.append(value)

                insert_query = insert_query.rstrip(',') + ")"
                # print("")
                # print(f"insert_query = {insert_query}")
                # print(datas)
                bytesToCommit += sys.getsizeof(datas)
                try:
                    # Execute the command to insert the data into the destination table
                    oracle_cursor.execute(insert_query, datas)
                except:
                    print(f"insert_query = {insert_query}")
                    print(datas)
                    oracle_connection.rollback()
                    oracle_cursor.close()
                    oracle_connection.close()
                    sql_server_cursor.close()
                    sql_server_connection.close()
                    quit()

                # To speed up execution, keep up to 10MB of data in the cache
                # before unloading and persisting it in the destination database.
                if bytesToCommit >= (1024*10):
                    oracle_connection.commit()
                    bytesToCommit = 0
        else:
            print("")
    else:
        print(f"When trying to create table {table_name}, the following error occurred: \"The identifier name started with an ASCII character other than a letter or number. After the first character of the identifier name, ASCII characters including \"$\", \"#\", and \"_\" are allowed. Double quotation marks can contain any character except double quotation marks. Alternative quotation marks (q'#...#') cannot use spaces, tabs, or line breaks as delimiters. For all other contexts, see the SQL Language Reference Manual.\"");

oracle_connection.commit()
oracle_cursor.close()
oracle_connection.close()
sql_server_cursor.close()
sql_server_connection.close()
