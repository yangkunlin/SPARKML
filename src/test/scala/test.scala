import utils.CosineUtil

/**
  * Description: 
  *
  * @author YKL on 2018/6/6.
  * @version 1.0
  *          spark:梦想开始的地方
  */
object test {

  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder
//      .appName("test")
//      .config("spark.sql.warehouse.dir", "spark-path")
//      .master("local[*]")
//      .getOrCreate()
//
//    HbaseServiceImpl.getSearch(spark)
//        .filter(row => {
//          !row.isNullAt(1) && row.length > 1
//        })
//      .filter(row => {
//      !row.getAs[String]("uid").isEmpty && !row.getAs[String]("key").isEmpty
//    })
//      .select("uid", "key")
//      .rdd
//      .map(row => {
//      (row.getAs[String]("uid"), row.getAs[String]("key").replaceAll("\n.*+", ""))
//    }).groupByKey().collect.foreach(println)

    println(CosineUtil.getSimilarity("杨昆霖", "杨昆"))

  }

}
