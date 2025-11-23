def conexao(senha,usuario,ip,sid):
    
    import oracledb    
    
    connection = oracledb.connect(
        user = usuario,
        password = senha,
        dsn = ip+'/'+sid
    )
    
    print("Successfully connected to Oracle Database")
    
    return connection

