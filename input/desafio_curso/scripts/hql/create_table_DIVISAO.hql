CREATE EXTERNAL TABLE IF NOT EXISTS DESAFIO_CURSO.TBL_DIVISAO ( 
        Division string,
        DivisionName string
    )
COMMENT 'Tabela de DIVISAO'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/DIVISAO/'
TBLPROPERTIES ("skip.header.line.count"="1");