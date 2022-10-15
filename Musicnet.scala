// Databricks notebook source
// Databricks notebook source
import org.apache.spark.sql.SparkSession
val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
val spark = SparkSession
   .builder()
   .appName("SparkoExample")
   .config("spark.sql.warehouse.dir", warehouseLocation)
   .enableHiveSupport()
   .getOrCreate()
 val data = sc.textFile("/FileStore/tables/musicnet.csv") 

// COMMAND ----------

//creer une case class
case class Music(id:String, composer:String, composition:String, movement:String, ensemble:String, source:String, transcriber:String, catalog_name:String, seconds:Integer)


// COMMAND ----------

val df = spark.read
              .option("header", true)
              .schema("ID String, Composer String, Composition String, Movement String, Ensemble String, Source String, Transcriber String, Catalog String, Seconds Integer")
              .csv("/FileStore/tables/musicnet_metadata.csv").toDF()

// COMMAND ----------

//Show the Data
//df.count()
df.show()
//Use printSchema Method
//df.printSchema()

// COMMAND ----------

//Using SQL function upon a SparkSession
df.createOrReplaceTempView("music")
spark.sql("select * from music ").show()

// COMMAND ----------

//1.1.1 Quel ensemble d’instruments a été utilisé lors de l'enregistrement?page 10
df.select("Ensemble").distinct().show()

// COMMAND ----------

//1.1.1 Quel ensemble d’instruments a été utilisé lors de l'enregistrement?page 10
spark.sql("select distinct (ensemble)from Music").show()


// COMMAND ----------

//1.1.2 Quels ensemble d’instruments a été utilisés le plus fréquemment?page 12
spark.sql("select Ensemble,count(ensemble) as Instruments from music group by ensemble order by count(ensemble) desc").limit(1).show()

// COMMAND ----------

//2.2.1 Combien de composition a été réalisée par chaque compositeur?page 16
spark.sql("select Composer, count(Composer) as NumOfCompositions from music group by composer order by NumOfCompositions desc"  ).show()

// COMMAND ----------

//2.2.1 Combien de composition a été réalisée par chaque compositeur?page 17
import org.apache.spark.sql.functions._
df.groupBy("Composer").count().show()

// COMMAND ----------

//2.2.2 Quel mouvement musical appartient-il chaque compositeur le plus ? page 19
spark.sql("select composer, movement , count ( movement)  from music group by composer, movement order by count( movement) desc").show()


// COMMAND ----------

////3.3.1 Quel est la durée maximale et minimale de chaque composition?page 23
df.groupBy( "Composition").agg(min("Seconds").alias("MinlDuration")).show()
//df.groupBy( "Composition").agg(max("Seconds").alias("MaxDuration")).show()

// COMMAND ----------

//3.3.2 Quel est la durée  moyenne  des compositions réalisée par le compositeur?page 27
df.groupBy( "Composer").agg(avg("Seconds").alias("AverageDuration")).show()
