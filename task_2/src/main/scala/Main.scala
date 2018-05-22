import java.net.URI
import java.nio.charset.Charset
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.SparkSession
import scala.util.{Failure, Success, Try}
import collection.JavaConverters._

object Main extends App {
  val spark = SparkSession.builder()
    .appName("Task2")
    .master("local[*]")
    .config("spark.driver.maxResultSize", "2g")
    .getOrCreate()

  val sc = spark.sparkContext

  val taskName = "task_2"
  val fileName = "data"
  val resultPath = Paths.get(s"result/$taskName.txt")
  val pathToProject = Paths.get(s"dataset/$fileName").toAbsolutePath.toString.replaceAll("\\\\", "/")
  val dataLocation = s"file:///$pathToProject"

  def map(str: String): Option[(String, String)] = {
    Try {
      val itr = str.split("\t").toIterator
      val id = itr.next()
      val rawHour = itr.next()
      val domain = Utils.getDomain(itr.next())
      (domain, rawHour)
    }.toOption

  }

  def reduce(seq: Seq[String]): Option[String] = {
    def trimDateTodd(str: String): String = {
      str.take(11)
    }

    def getHH(str: String): String = {
      Utils.getJavaDate(str).getHours.toString
    }

    def getActiveHours(seq: Seq[String]): Int = {
      val numbOfVisits = seq.length
      val threshold = Math.round(numbOfVisits * 0.15)

      seq.groupBy(getHH).mapValues(_.length).count(p => p._2 >= threshold)
    }

    val newDf = new SimpleDateFormat("yyyy MM dd HH")

    Try(seq.groupBy(trimDateTodd).mapValues(getActiveHours).values.sum.toString).toOption
  }

  def saveResult(path: Path, map: Seq[(String, String)]): Unit = {
    if (path.toFile.exists) path.toFile.delete()
    val iter = map.map(p => s"${p._1}: ${p._2}").asJava
    Files.write(path, iter , Charset.defaultCharset(), StandardOpenOption.CREATE)
  }

  val result = sc.textFile(dataLocation).flatMap(map).groupBy(_._1).mapValues(_.map(_._2).toList).mapValues(reduce).filter(_._2.nonEmpty).mapValues(_.get).sortBy(v => -v._2.toInt).collect().take(10)
  saveResult(resultPath, result)
}

object Utils {
  def getDomain(url: String): String = {
    val uri = Try(new URI(url)).getOrElse(urlFromStringWithIllegalCharacters(url))
    val host = uri.getHost
    if (host.startsWith("www.")) host.drop(4) else host
  }

  def getJavaDate(time: String): Date = {
    val formatter = new SimpleDateFormat("yyyy MM dd HH:mm:ss.SSS")
    formatter.parse(time)
  }

  def stringPairAsText(v1: String, v2: String): String = {
    v1 + separator + v2
  }

  def textAsStringPair(text: String): (String, String) = {
    val arr = text.split(separator)
    (arr(0), arr(1))
  }

  private def urlFromStringWithIllegalCharacters(str: String): URI = {
    val ponetnialUri = str.dropRight(1)
    Try(new URI(ponetnialUri)) match {
      case Success(uri) => new URI(ponetnialUri)
      case Failure(_: java.net.URISyntaxException) => urlFromStringWithIllegalCharacters(ponetnialUri)
      case _ => new URI("")
    }
  }

  val separator = "\t"

}