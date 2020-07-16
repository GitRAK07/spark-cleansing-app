package com.kloud9.scalabasics

object conditional_statements extends App  {
  println("Enter your Birth Year to find your age and its category:")
  var dob = scala.io.StdIn.readInt()
  var age = 2020-dob
  if(age>10&&age<20){
    print(s"Your age is ${age} and you are a Teenager!")
  }
  else if(age>20&&age<40){
    println(s"Your age is ${age} and you are a Youngster")
  }

  else if(age>40&&age<60){
    println(s"Your age is ${age} and you are Uncle/Aunty")
  }
  else if(age>60&&age<80){
    println(s"Your age is ${age} and you are Thatha/Paati")
  }
  else{
    println("Stop lying to me !!!!!")
  }
  // If Else statement to return value --  Start
  def checkIt (a:Int)  =  if (a >= 0) 1 else -1    // Passing a if expression value to function

  val result = checkIt(-10)
  println (result)
  // Else statement to return value --  End
  val bool = true

  val evar= if (bool) "True" else 0 //IF expression
  print(evar)

}



