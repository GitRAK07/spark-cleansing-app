package com.kloud9.scalabasics

object while_doWhile {
  def main(args: Array[String]): Unit = {
    println("Enter the first number and range to get the consecutive list of 10")
    var initial= scala.io.StdIn.readInt()
    var range= scala.io.StdIn.readInt()
    var count = 0
    var output=0
    println("Using While Statement")
    println(initial)

    while(count<=10){
      count= count +1
      output= initial+(range*count)
      println(output)
    }
    count = 0
    output = 0
    println("Using Do-While Statement")
    println(initial)
    do {
      count= count +1
      output= initial+(range*count)
      println(output)
    } while(count<=10)
  }

}
