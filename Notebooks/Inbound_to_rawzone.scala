// Databricks notebook source
// MAGIC %python
// MAGIC spark

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls('mnt/dados/inbound/')

// COMMAND ----------

// MAGIC %md
// MAGIC **Leitura de dados imoveis**

// COMMAND ----------

val path="dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
val dados= spark.read.json(path)

// COMMAND ----------

// MAGIC %md
// MAGIC **Visualização de dados imoveis**

// COMMAND ----------

// MAGIC %sql
// MAGIC display(dados)

// COMMAND ----------

// MAGIC %md
// MAGIC **Removendo colunas**

// COMMAND ----------

val dados_anuncios=dados.drop("imagens","usuario")
display(dados_anuncios)

// COMMAND ----------

// MAGIC %md
// MAGIC **Criando Coluna de ID**

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

val df_rawzone = dados_anuncios.withColumn("Id",col("anuncio.id"))
display(df_rawzone)

// COMMAND ----------

// MAGIC %md
// MAGIC **Salvando na camada bronze nos conteiners de arquivos do azure**

// COMMAND ----------

val path= "dbfs:/mnt/dados/bronze/dataset_imoveis"
df_rawzone.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------

// MAGIC %md
// MAGIC **Uma das principais vantagens dos arquivos Delta é a capacidade de controle de versões. A camada de logs transacionais, conhecida como "delta_log" e transações ACID**
