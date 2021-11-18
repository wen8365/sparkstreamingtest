// 连接到datasorce传输
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.appName("mysparkapp").getOrCreate()
val lines = spark.readStream.format("socket").option("host", "121.37.94.163").option("port", 5050).load()

// 导入基站数据
val stationDF = spark.read.option("header", "true").csv("hdfs://master1:9000/user/ccgroup/station_info.csv")

// 注意： 需要导入隐式转换
import spark.implicits._
//使用内置函数split分割列
val columns = split($"value", ",")
var newDF = { lines.withColumn("timsStamp", regexp_replace(columns.getItem(0), " ", ""))
.withColumn("imsi", regexp_replace(columns.getItem(1), " ", ""))
.withColumn("lac_id", regexp_replace(columns.getItem(2), " ", ""))
.withColumn("cell_id", regexp_replace(columns.getItem(3), " ", ""))
.drop("value")
}
newDF = newDF.where("imsi <> '' and lac_id <> '' and cell_id <> '' and imsi rlike '^[0-9]{18}$'")
newDF = newDF.withColumn("timsStamp", $"timsStamp".cast("float")/1000.0)
newDF = newDF.filter($"timsStamp" > 1538496000.0 && $"timsStamp" < 1538582400.0)
newDF = newDF.withColumn("datetime", from_unixtime($"timsStamp", "yyyyMMddHHmmss"))
newDF = newDF.withColumn("lac_cell", concat_ws("-", $"lac_id", $"cell_id"))
var groupDF = newDF.groupBy("lac_cell").count()
var joinDF = groupDF.join(stationDF, $"lac_cell" === $"laci")
//val query = joinDF.writeStream.outputMode("update").format("console").start()

// 写入kafka
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
val query = { joinDF.writeStream.outputMode("complete").foreachBatch{
    (batchDF:Dataset[Row], batchId: Long) => batchDF.persist()
    batchDF.selectExpr("laci AS key", "to_json(struct(longitude,latitude,count)) AS value")
           .write
           .format("kafka")
           .option("kafka.bootstrap.servers", "master1:9092")
           .option("topic", "sparkstreaming")
           .option("checkpointLocation", "/home/ccgroup/sparkstreamingoutput")
           .save()
    batchDF.unpersist()
    // scala 2.12 Row 类型转换不向下兼容，此处增加() 或者 println(1)修复报错
    ()
}.start()
}