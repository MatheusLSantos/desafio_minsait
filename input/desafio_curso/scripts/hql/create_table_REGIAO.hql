CREATE EXTERNAL TABLE IF NOT EXISTS DESAFIO_CURSO.TBL_REGIAO ( 
        RegionCode string,
        RegionName string
    )
COMMENT 'Tabela de REGIAO'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/REGIAO/'
TBLPROPERTIES ("skip.header.line.count"="1");