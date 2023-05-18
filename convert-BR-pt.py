from datetime import datetime
import sys
import pyodbc
import cx_Oracle
import keyring

# Caracteres não permitidos no início do nome das tabelas
caracNaoPermitidos = ["$","#","_"]
# Indica se as tabelas já existes na origem deverão ser recridas
recriar=False

# Informe os dados para conexão nas strings abaixo
sql_server_connection_string = keyring.get_password("sqlserver", "connectionstring")
oracle_connection_string = keyring.get_password("sqloracle", "connectionstring")

sql_server_connection = pyodbc.connect(sql_server_connection_string)
oracle_connection = cx_Oracle.connect(oracle_connection_string)

sql_server_cursor = sql_server_connection.cursor()

# Busca todas as tabelas da base origem (SQL Server) em ordem alfabética
sql_server_cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME")
tables = sql_server_cursor.fetchall()

# Processa cada uma das tabelas
for table in tables:
    table_name = table[0]
    nomeDeTabelaRuim = False
    try:
        nomeDeTabelaRuim = caracNaoPermitidos.index(table_name[:1]) >= 0
    except ValueError:
        nomeDeTabelaRuim = False

    if nomeDeTabelaRuim == False:
        print(f"Processando tabela {table_name.upper()}...", end='')

        # Verifica se a tabela já existe na base destino
        oracle_cursor = oracle_connection.cursor()
        oracle_cursor.execute(f"SELECT count(*) FROM user_tables WHERE table_name = '{table_name.upper()}'")
        table_exists = oracle_cursor.fetchone()[0] > 0
        oracle_cursor.close()

        # Verifica se a tabela já existe e se será necessário recriá-la
        if table_exists and recriar :
            # Seleciona todas as constraints que existam na tabela destino e desabilita para evitar problemas ao dropar a tabela
            oracle_cursor = oracle_connection.cursor()
            oracle_cursor.execute(f"SELECT constraint_name FROM all_constraints WHERE table_name = '{table_name.upper()}' AND constraint_type = 'R'")
            for constraint_name in oracle_cursor:
                try:
                    oracle_cursor.execute(f"ALTER TABLE {table_name} DISABLE CONSTRAINT {constraint_name[0]}")
                except:
                    print(f"Erro desabilitando constraint {constraint_name[0]}")
                    quit()

            oracle_cursor.close()

            # Dropa a tabela no destino
            print(f" Dropando a tabela, já existente, {table_name.upper()} => DROP TABLE {table_name}...", end='')
            oracle_cursor = oracle_connection.cursor()
            try:
                oracle_cursor.execute(f"DROP TABLE {table_name}")
            except:
                print(f"Erro dropando tabela")
                quit()

            oracle_cursor.close()
            oracle_connection.commit()
        else:
            if table_exists:
                print(f" Pulando, tabela já existente.", end='');

        # Verifica se devemos prosseguir na criação da tabela no destino, e na inclusão dos dados da tabela origem para a tabela destino
        if table_exists == False or recriar:
            print("")

            # Seleciona as colunas, tipos, tamanhos, se permite nulo, se é primary key etc para montagem do comando de CREATE TABLE no destino
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

                # Faz o de/para dos tipos de dados do SQL Server (origem) para os tipos de dados do Oracle (destino), seus equivalentes mais adequados
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
                # Executa a criação da tabela no destino
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

            # Cria o reader dos dados da tabela origem
            sql_server_cursor.execute(f"SELECT * FROM {table_name}")
            rows = sql_server_cursor.fetchall()

            for row in rows:
                # Inicia o comando INSERT para inserir os dados na tabela destino
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

                    # Trata alguns dados para evitar erros nos casos de coluna não permitir nulo. 
                    # Por exemplo, no SQL Server uma coluna varchar que não permite nulo, aceita o conteúdo "", 
                    # o Oracle não permite, é necessário um espaço em branco " "
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
                    # Executa o comando para inserir os dados na tabela destino
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

                # Para a execução ser mais rápida, mantem em cache de dados até 10MB de dados 
                # antes de descarregar e efetivas a persistência na base destino.
                if bytesToCommit >= (1024*10):
                    oracle_connection.commit()
                    bytesToCommit = 0
        else:
            print("")
    else:
        print(f"Ao tentar criar a tabela {table_name}, ocorreu o seguinte erro: \"O nome do identificador começou com um caractere ASCII diferente de uma letra ou número. Após o primeiro caractere do nome do identificador, são permitidos caracteres ASCII, incluindo \"$\", \"#\" e \"_\". Identificadores entre aspas duplas podem conter qualquer caractere, exceto aspas duplas. Aspas de citação alternativas (q'#...#') não podem usar espaços, tabulações ou quebras de linha como delimitadores. Para todos os outros contextos, consulte o Manual de Referência da Linguagem SQL.\"");

oracle_connection.commit()
oracle_cursor.close()
oracle_connection.close()
sql_server_cursor.close()
sql_server_connection.close()
