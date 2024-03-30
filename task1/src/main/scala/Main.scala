import org.apache.spark.sql.SparkSession
import java.io.File
import org.slf4j.LoggerFactory

object Main {
  private val logger = LoggerFactory.getLogger(Main.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("task1")
      .master("local[*]")
      .getOrCreate()

    try {
      val ordersDF = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("./data/orders.csv")

      val customersDF = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("./data/customers.csv")

      val joinedDF = ordersDF.join(customersDF, "customer_id")

      joinedDF.coalesce(1)
        .write
        .option("header", "true")
        .mode("overwrite")
        .csv("./data/output")

      renameOutputFile("./data/output", "output.csv")

    } catch {
      case e: Exception => logger.error("An error occurred in processing", e)

    } finally {
      spark.stop()
    }
  }

  def renameOutputFile(directoryPath: String, newFileName: String): Unit = {
    val dir = new File(directoryPath)
    val files = dir.listFiles()
    if (files != null) {
      files.foreach { file =>
        if (file.getName.startsWith("part-") && file.getName.endsWith(".csv")) {
          val newFile = new File(dir, newFileName)
          if (!file.renameTo(newFile)) {
            logger.error(s"Failed to rename ${file.getName}")
          } else {
            logger.info(s"Renamed ${file.getName} to ${newFile.getName}")
          }
        }
      }
    } else {
      logger.error(s"Directory $directoryPath does not exist or is not accessible.")
    }
  }
}