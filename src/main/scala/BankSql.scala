
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

// One method for defining the schema of an RDD is to make a case class with the desired column
// names and types.
case class Bank(age: Integer, job: String, marital: String, education: String, balance: Integer)

object BankSql {
  def main(args: Array[String]) {


    val sparkConf = new SparkConf().setAppName("BankSQL").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val bankText = sc.textFile("src/main/resources/bank-full.csv")

    val bank = bankText.map(s => s.split(";")).filter(s => s(0) != "\"age\"").map(
      s => Bank(s(0).toInt,
        s(1).replaceAll("\"", ""),
        s(2).replaceAll("\"", ""),
        s(3).replaceAll("\"", ""),
        s(5).replaceAll("\"", "").toInt
      )
    )

    bank.toDF().registerTempTable("bank")

    //sqlContext.sql("SELECT * FROM bank where age < 30").collect().foreach(println)

    sqlContext.sql("select age, count(1) " +
      "from bank where age < 30 group by age order by age").collect().foreach(println)

  }
}