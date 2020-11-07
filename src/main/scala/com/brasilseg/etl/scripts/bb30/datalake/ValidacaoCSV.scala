package com.brasilseg.etl.scripts.bb30.datalake

import java.io.BufferedReader
import java.io.FileReader

object ValidacaoCSV {

  private val QUOTE: String = "\""

  private val SEPARATOR: Char = '§'

  var countTotalLines: Int = 0

  def main(args: Array[String]): Unit = {

    val columns: Int = java.lang.Integer.parseInt(args(1))
    val reader: BufferedReader = new BufferedReader(new FileReader(args(0)))
    var countLinesError: Int = 0
    var multiLine = getMultiLine(reader)
    while (multiLine != null) {
      val line = multiLine.toString
      if (line.count(_ == SEPARATOR) + 1 != columns) {
        countLinesError += 1;
        println("%d: %s".format(countTotalLines, line))
      }
      multiLine = getMultiLine(reader)
    }
    reader.close()

    println()
    println("Linhas analisadas = %d".format(countTotalLines))
    println("Linhas quebradas = %d".format(countLinesError))
  }

  private def getMultiLine(reader: BufferedReader): String = {
    var line = nextLine(reader)
    if (line != null) {
      val result = new StringBuilder
      var posStart = 0
      var start = true
      while (start) {
        posStart = line.indexOf(QUOTE)
        if (posStart >= 0) {
          result.append(line.substring(0, posStart + 1))
          line = line.substring(posStart + 1, line.length)
          var end = true
          while (end) {
            val posEnd = line.indexOf(QUOTE)
            if (posEnd >= 0) {
              result.append(line.substring(0, posEnd + 1))
              line = line.substring(posEnd + 1, line.length)
              end = false
            } else {
              result.append(line)
              line = nextLine(reader)
            }
          }
        } else {
          start = false
        }
      }
      result.append(line)
      return result.toString
    }
    null
  }

  private def nextLine(reader: BufferedReader): String = {
    countTotalLines += 1;
    reader.readLine()
  }

}