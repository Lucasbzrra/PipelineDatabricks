// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.fs.ls("dbfs:/mnt/dados/bronze/dataset_imoveis")

// COMMAND ----------

// MAGIC %md
// MAGIC **Leitura de dados no formato delta**

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
val dados = spark.read.format("delta").load(path)

// COMMAND ----------

var dadosdetalhados = dados.select("anuncio.*","anuncio.endereco")

// COMMAND ----------

// MAGIC %md
// MAGIC **Removendo a coluna "caracteristicas"**

// COMMAND ----------

var dadossilver=dadosdetalhados.drop("caracteristicas","endereco")

// COMMAND ----------

// MAGIC %md
// MAGIC **Salvando dados na camada silver**

// COMMAND ----------

val path = "dbfs:/mnt/dados/silver/dataset_imoveis"
dadossilver.write.format("delta").mode("overwrite").save(path)
