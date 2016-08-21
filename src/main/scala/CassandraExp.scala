
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkConf, SparkContext}

case class User(userid: String, first_name: String, last_name: String)

object CassandraExp {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .set("spark.cassandra.connection.host", "localhost")
      .setAppName("CassandraExp")
      .setMaster("local")

    val sc = new SparkContext(sparkConf)
    /*
    val users = sc.cassandraTable[User]("imaya", "users")
    print(users.first())
    */
    val cctx = new CassandraSQLContext(sc)
    val users = cctx.sql("SELECT * FROM imaya.users")
    print(users.first())
  }
}
