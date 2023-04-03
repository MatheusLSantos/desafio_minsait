CREATE EXTERNAL TABLE IF NOT EXISTS DESAFIO_CURSO.TBL_CLIENTES ( 
        AddressNumber string,
        BusinessFamily string,
        BusinessUnit string,
        Customer string,
        CustomerKey string,
        CustomerType string,
        Division string,
        LineofBusiness string,
        Phone string,
        RegionCode string,
        RegionalSalesMgr string,
        SearchType string
    )
COMMENT 'Tabela de CLIENTES'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/CLIENTES/'
TBLPROPERTIES ("skip.header.line.count"="1");