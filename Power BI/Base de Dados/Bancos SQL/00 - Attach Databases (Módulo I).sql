/*
|----------------------------------------------------------------------------					      
| Manter os arquivos no diretório C:\BANCOS\ 
| Conectado como SA 
| Configurar Permissões da pasta no windows
| Autor: Hélio de Almeida										                           
| Criação: 20/1/2010 - Modificação: 21/4/2017
|----------------------------------------------------------------------------

*/


-----------------------------------------------------------------------------		
		EXEC SP_ATTACH_DB 
			@DBNAME ='BDSisDep',
			@FILENAME1 = 'C:\BANCOS\BDSisDep.mdf',
			@FILENAME2 = 'C:\BANCOS\BDSisDep_log.ldf';
-----------------------------------------------------------------------------		
		EXEC SP_ATTACH_DB 
			@DBNAME ='BDClientes',
			@FILENAME1 = 'C:\BANCOS\BDclientes.mdf',
			@FILENAME2 = 'C:\BANCOS\BDclientes_log.ldf';

-----------------------------------------------------------------------------			
		EXEC SP_ATTACH_DB 
			@DBNAME ='BDSeguroVeiculo',
			@FILENAME1 = 'C:\BANCOS\BDSeguroVeiculo.mdf',
			@FILENAME2 = 'C:\BANCOS\BDSeguroVeiculo_log.ldf';
-----------------------------------------------------------------------------
		EXEC SP_ATTACH_DB 
			@DBNAME ='BDSysConVendas',
			@FILENAME1 = 'C:\BANCOS\BDSysConVendas.mdf',
			@FILENAME2 = 'C:\BANCOS\BDSysConVendas_log.ldf';
-----------------------------------------------------------------------------
	GO
	DBCC SHRINKDATABASE (N'BDSisDep');
	GO
	DBCC SHRINKDATABASE (N'BDclientes');
	GO
	DBCC SHRINKDATABASE (N'BDSeguroVeiculo');
	GO
	DBCC SHRINKDATABASE (N'BDSysConVendas');
	GO
-----------------------------------------------------------------------------
	SELECT 'BANCOS INSTALADOS e Compactados!!!' AS Confirmação
