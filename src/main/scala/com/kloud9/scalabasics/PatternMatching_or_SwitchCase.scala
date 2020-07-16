package com.kloud9.scalabasics

object PatternMatching_or_SwitchCase {

  def main(args: Array[String]): Unit ={
    println("Select any of the below option to start:")
    println("1. Motivational quote")
    println("2. Love Quote")
    println("3. Warrior Quote")

    var option = scala.io.StdIn.readInt()
    option match{
      case 1 => println("The secret of getting ahead is getting started!")
      case 2 => println("You always gain by giving love")
      case 3 => println("A warrior must only take care that his spirit is never broken")
      case _ => println("Sorry! You have blindness!")
    }
  }


}
