CREATE EXTERNAL TABLE IF NOT EXISTS DESAFIO_CURSO.TBL_ENDERECO ( 
        AddressNumber string,
        City string,
        Country string,
        CustomerAddress1 string,
        CustomerAddress2 string,
        CustomerAddress3 string,
        CustomerAddress4 string,
        State string,
        ZipCode string
    )
COMMENT 'Tabela de ENDERECO'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/ENDERECO/'
TBLPROPERTIES ("skip.header.line.count"="1");