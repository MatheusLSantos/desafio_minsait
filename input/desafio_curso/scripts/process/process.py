from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f
import os
import re
from os import system

spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

# Criando dataframes diretamente do Hive
df_clientes = spark.sql("SELECT * FROM DESAFIO_CURSO.TBL_CLIENTES where not CustomerKey='CustomerKey'")
df_divisao = spark.sql("SELECT * FROM DESAFIO_CURSO.TBL_DIVISAO where not Division='Division'")
df_endereco = spark.sql("SELECT * FROM DESAFIO_CURSO.TBL_ENDERECO where not AddressNumber='AddressNumber'")
df_regiao = spark.sql("SELECT * FROM DESAFIO_CURSO.TBL_REGIAO where not RegionCode='RegionCode'")
df_vendas = spark.sql("SELECT * FROM DESAFIO_CURSO.TBL_VENDAS where not OrderNumber='OrderNumber'")

# Função para substituir Null por 0
def filtrarVaziosENulos(df):
    listaColunas = df.columns # Lista de colunas do dataframe
    
    # laço de repetição que percorre cada coluna do dataframe substituindo nulo por 0
    for coluna in listaColunas:
        colunaAntiga="old"+coluna
        df = df.withColumnRenamed(coluna, colunaAntiga) # Renomeando a coluna sem modificações
        # Criando uma nova coluna com os dados nulos substituídos por 0
        df = df.withColumn(coluna, when((col(colunaAntiga)).isNotNull(), col(colunaAntiga)).otherwise(lit(0)))
        df = df.drop(colunaAntiga) # Removendo coluna sem as modificações
        
    # laço de repetição que percorre cada coluna do dataframe substituindo vazios por 'Não informado'
    for coluna in listaColunas:
        colunaAntiga="old"+coluna
        df = df.withColumnRenamed(coluna, colunaAntiga) # Renomeando a coluna sem modificações
        # Criando uma nova coluna com as strings vazias substituídas por 'Não informado'
        df = df.withColumn(coluna, when(trim(col(colunaAntiga))=="","Não informado").otherwise(col(colunaAntiga)))
        df = df.drop(colunaAntiga) # Removendo coluna sem as modificações
    
    return df

# Salvando o stage no datalake/silver
def salvar_silver(df, file):
    output = "/datalake/silver/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    
    os.system(erase) # Limpando pasta datalake/silver no hdfs
    
    # Exportando arquivos para a pasta datalake/silver no hdfs
    df.coalesce(1).write\
        .format("csv")\
        .option("header", True)\
        .option("delimiter", ";")\
        .mode("overwrite")\
        .save("/datalake/silver/"+file+"/")

# Espaço para tratar e juntar os campos e a criação do modelo dimensional
df_clientes = filtrarVaziosENulos(df_clientes)
df_divisao = filtrarVaziosENulos(df_divisao)
df_endereco = filtrarVaziosENulos(df_endereco)
df_regiao = filtrarVaziosENulos(df_regiao)
df_vendas = filtrarVaziosENulos(df_vendas)

df_stage = df_vendas.join(df_clientes, df_vendas.customerkey == df_clientes.customerkey, "left").drop(df_clientes.customerkey)
df_stage = df_stage.join(df_endereco, df_stage.addressnumber == df_endereco.addressnumber, "left").drop(df_endereco.addressnumber)
df_stage = df_stage.join(df_divisao, df_stage.division == df_divisao.division, "left").drop(df_divisao.division)
df_stage = df_stage.join(df_regiao, df_stage.regioncode == df_regiao.regioncode, "left").drop(df_regiao.regioncode)

listaColunas = df_stage.columns # Lista de colunas do dataframe
# laço de repetição que percorre cada coluna do dataframe substituindo os valores nulos decorrentes do join por 'Não informado'
for coluna in listaColunas:
    colunaAntiga="old"+coluna
    df_stage = df_stage.withColumnRenamed(coluna, colunaAntiga) # Renomeando a coluna sem modificações
    # Criando uma nova coluna com os dados nulos substituídos por 'Não informado'
    df_stage = df_stage.withColumn(coluna, when(col(colunaAntiga).isNull(),"Não informado").otherwise(col(colunaAntiga)))
    df_stage = df_stage.drop(colunaAntiga) # Removendo coluna sem as modificações

salvar_silver(df_stage, "stage")

# Adicionando chaves PK
df_stage = df_stage.withColumn("PK_CLIENTES", sha2(concat_ws("", col("businessfamily"), col("businessunit"), col("customer"), col("customertype"), col("customerkey"), col("phone")), 256))
df_stage = df_stage.withColumn("PK_TEMPO", sha2(concat_ws("", col("actualdeliverydate"), col("datekey"), col("invoicedate"), col("promiseddeliverydate")), 256))
df_stage = df_stage.withColumn("PK_LOCALIDADE", sha2(concat_ws("", col("addressnumber"), col("city"), col("country"), col("customeraddress1"), col("customeraddress2"), col("customeraddress3"), col("customeraddress4"), col("divisionname"), col("regioncode"), col("regionname"), col("state"), col("zipcode")), 256))
df_stage = df_stage.dropDuplicates()

# Verificando se o stage ainda possui nulos
df_contagem = df_stage.select([count(when(col(c).isNull(),c)).alias(c) for c in df_stage.columns]) # Quantidade de nulos em cada coluna
df_contagem.show(truncate=False)

# criando o fato
ft_vendas = df_stage.select("item", "listprice", "ordernumber", "PK_CLIENTES", "PK_LOCALIDADE", "PK_TEMPO", "salesamount", "salesmarginamount", "salesprice", "salesquantity")

#criando as dimensões
dim_clientes = df_stage.select("customer", "customerkey", "phone", "PK_CLIENTES")
dim_tempo = df_stage.select("actualdeliverydate", "datekey", "invoicedate", "PK_TEMPO", "promiseddeliverydate")
dim_localidade = df_stage.select("addressnumber", "city", "country", "customeraddress1", "customeraddress2", "customeraddress3", "customeraddress4", "divisionname", "PK_LOCALIDADE", "regionname", "state")

# função para salvar os dados na gold
def salvar_df(df, file):
    output = "/datalake/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/desafio_curso/gold/"+file+".csv"
    print(rename)
    
    os.system(erase)
    
    df.coalesce(1).write\
        .format("csv")\
        .option("header", True)\
        .option("delimiter", ";")\
        .mode("overwrite")\
        .save("/datalake/gold/"+file+"/")

    os.system(rename)

#Salvando fato e dimensões na gold
salvar_df(ft_vendas, 'FT_VENDAS')
salvar_df(dim_clientes, 'DIM_CLIENTES')
salvar_df(dim_tempo, 'DIM_TEMPO')
salvar_df(dim_localidade, 'DIM_LOCALIDADE')