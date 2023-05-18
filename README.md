
# MigrationSQLServerToOracle

Follow the steps below:
  - Edit the setpass.py script with the connection data for the SQL Server and Oracle databases involved.
  - Execute it:
        - > python setpass.py
        * This step will create encrypted connection string records that will be retrieved during the execution of convert-US-en.py.
  - Edit the convert-US-en.py file and decide whether you want the tables to be recreated or ignored in case they exist in the destination.
  - Execute it:
        - > python convert-US-en.py
        * This step will create the tables in the destination and populate them with the same data from the source database.
The creation of relationship constraints has not been developed yet.

-------------------------------------------------------------------------------------------------------------------------------------------------

# MigrationSQLServerToOracle

Siga os passos abaixo:
  - Edite o script setpass.py com os dados das conexões dos bancos de dados SQL Server e Oracle envolvidos
  - Execute-o : 
        - > python setpass.py
        * Este passo irá criar os registros de string de conexão cryptografados que será recuperado na execução do convert-BR-pt.py
  - Edite o arquivo convert-BR-pt.py e decida se deseja ou não que as tabelas, no caso de existirem no destino, serão recriadas ou ignoradas
  - Execute-o:
        - > python convert-BR-pt.py
        * Este passo irá criar as tabelas no destino e popular com os mesmos dados da base origem.
Ainda não foi desenvolvido a criação de constraintsde relacionamento.

