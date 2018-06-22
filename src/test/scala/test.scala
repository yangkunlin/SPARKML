import org.apache.spark.sql.SparkSession
import service.MysqlServiceImpl
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
    val sparkSession = SparkSession.builder
      .appName("test")
      .config("spark.sql.warehouse.dir", "spark-path")
      .master("local[*]")
      .getOrCreate()
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

    val tmpRdd = MysqlServiceImpl.getTableDF(sparkSession, "video").select("videoID", "videoName").rdd.map(row => (row.get(0), row.get(1))).cache

    val mapA = tmpRdd.collect.toMap
    val mapB = tmpRdd.collect.toMap
    var tmp = 0.0
    var tmpName = ""
    var nameA = ""
    var nameB = ""
    mapA.foreach(_tuple => {
      var score = 0.0
      nameA = _tuple._2.toString
      mapB.foreach(_tuple => {
        nameB = _tuple._2.toString
        tmp = CosineUtil.getSimilarity(nameA, nameB)
        if (!nameA.equals(nameB) && score < tmp) {
          score = tmp
          tmpName = nameB
        }
      })
      println(nameA, " ", tmpName)
    })

//    println(CosineUtil.getSimilarity("杨昆霖", "杨昆"))

  }

}
