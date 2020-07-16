package com.kloud9.training
import org.scalatest.FunSuite
import org.scalatest.Assertions._
class DataTransformationUnitTest extends FunSuite  {

  test("An empty Set should have size 0") {


    assert(Set.empty.size == 0)
    println("The test is passed")
  }
  test("An empty Set should not have size other than 0") {
    assert(Set.empty.size == 2)
  }
  test("Checking Schema Comparision")
  {

  }
}
