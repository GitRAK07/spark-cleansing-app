package com.kloud9.scalabasics

object ClassExample extends App {

  val car = new Car("Tata","2020") //Instantiating a class ( Allocating memory)
  //On every instatiation the block will be executed
  println(car.customer)
  println("You have bought the below vehicle")
  println(car.brand, car.model)
  car.this_keyword_usage("BMW")


}

class Car(val brand: String, val model: String){
  print("Hello,")
  val customer = "Anand"
  //Method ( It is defined inside the Class. If not functino )
  def this_keyword_usage(brand: String): String ={
    println (s"The news is ${this.brand} acquired ${brand}")
    return this.brand
  }
  def no_this_keyword(): Unit = println(s"The brand name is ${brand}")
  def this_keyword_usage(): Int = {
    println("hello AnandKumar")
    return 2
  }
}
class Cars(name: String,age: Int ) //Constructor arguments should be declared using the keyword "val"