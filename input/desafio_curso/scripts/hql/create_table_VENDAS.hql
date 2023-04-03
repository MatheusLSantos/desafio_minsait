CREATE EXTERNAL TABLE IF NOT EXISTS DESAFIO_CURSO.TBL_VENDAS ( 
        ActualDeliveryDate string,
        CustomerKey string,
        DateKey string,
        DiscountAmount string,
        InvoiceDate string,
        InvoiceNumber string,
        ItemClass string,
        ItemNumber string,
        Item string,
        LineNumber string,
        ListPrice string,
        OrderNumber string,
        PromisedDeliveryDate string,
        SalesAmount string,
        SalesAmountBasedonListPrice string,
        SalesCostAmount string,
        SalesMarginAmount string,
        SalesPrice string,
        SalesQuantity string,
        SalesRep string,
        U_M string
    )
COMMENT 'Tabela de VENDAS'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/VENDAS/'
TBLPROPERTIES ("skip.header.line.count"="1");