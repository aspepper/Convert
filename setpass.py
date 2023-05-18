import keyring

keyring.set_password("sqlserver", "connectionstring", "DRIVER={ODBC Driver 17 for SQL Server};SERVER=serverIP;DATABASE=CORPDES_500;UID=username;PWD=XXXX")
keyring.set_password("sqloracle", "connectionstring", "username/pwd@SID")
