package com.kloud9.scalabasics

object for_loop {

  def main(args: Array[String]): Unit = {
    // using TO keyword and without {}
    for (i <- 1 to 10)
      println(i)
      println("exit")
    // using TO keyword
    for (i <- 1 to 10) {
      println(i)
      println("exit")
    }
    //using until keyword
    for (i <- 1 until 4) {
      println(i)

    }

  }

}
