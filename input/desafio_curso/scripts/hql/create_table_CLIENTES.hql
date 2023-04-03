CREATE EXTERNAL TABLE IF NOT EXISTS DESAFIO_CURSO.TBL_CLIENTES ( 
        Address Number string,
        Business Family string,
        Business Unit string,
        Customer string,
        CustomerKey string,
        Customer Type string,
        Division string,
        
    )
COMMENT 'Tabela de CLIENTES'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/CLIENTES/'
TBLPROPERTIES ("skip.header.line.count"="1");