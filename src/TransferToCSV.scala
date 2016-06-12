import java.io.{BufferedWriter, File, FileWriter}

import scala.collection.mutable.ListBuffer
import scala.io.Source

object FreeDBToCSV extends App {
  var result = ListBuffer[String]()
  val path = "D:/freedb-complete-20160601.tar/folk/"
  val out = "D:/freedb-complete-20160601.tar/folk.csv"

  val files = new File(path).listFiles()
  val total = files.length
  var start = System.nanoTime()
  for (i <- files.indices)
  {
    i % 100 match {
      case 0 => {
        println(i + "/" + total + " took: " + (System.nanoTime() - start) / 1000000000 + " s")
        start = System.nanoTime()
      }
      case _ =>
    }

    val file = files(i)
    file.isFile match {
      case true =>  {
        process(file)
      }
      case _ =>{

      }
    }
  }


  writecsv



  def process(file: File)= {
    val content = Source.fromFile(file, "iso-8859-1")
    val lines = content.getLines()
    val discId = value(lines, "DISCID")
    val discTitle = value(lines, "DTITLE")
    val year = value(lines,"DYEAR")
    val genre = value(lines,"GENRE")
    val titles = lines.filter(line => line.contains("TITLE")).map(item => item.replace(",","")).map(item =>  {
      item.split("=").length match {
        case 0 => item
        case 1 => item
        case _ => item.split("=")(1)
      }
    })

    result += List(discId,discTitle,year,genre,titles.mkString(" ")).mkString(",")
  }

  def value(lines: Iterator[String], key: String) = {
    lines.find(line => line.contains(key)) match {
      case None =>  ""
      case e:Some[String] =>  {
        val str = e.get
          str.split("=").length match {
            case 0 => ""
            case 1 => ""
            case _ => str.split("=")(1)
          }
      }
    }
  }

  def writecsv = {
    // PrintWriter
    val file = new File(out)
    val bw = new BufferedWriter(new FileWriter(file))
    result.foreach(r => {
      bw.write(r + "\n")
    })
    bw.close()
  }
}
