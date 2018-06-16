package com.scala.practice

import java.time.LocalDateTime

object Testcase {

  case class Cash1(amount: Double, currency: String, du: LocalDateTime)

  case class Address(city: String, country: String)

  case class Person(name: String, age: Int, address: Address)

  def fact(n : Int) : Int = n match {
    case 0 => 1
    case m => m * fact(m-1)
  }
  
  def main(args: Array[String]) {
    val c1 = Cash1(3000.0, "GBP", LocalDateTime.now)

    c1 match {
      case Cash1(v, "USD", _) => println("US cashflow")
      case Cash1(v, "GBP", _) => println(s"Uk cashflow: $v")

    }

    val a1 = Address("London", "UK")
    val a2 = Address("New York", "USA")

    val p1 = Person("fred", 41, a1)
    val p2 = Person("jane", 23, a2)

    p1 match {
      case Person(n, _, Address("London", _)) => println("$n liven in London")
      case _ => println("nothing")
    }
    
    p2 match {
      case Person(n, a, _) if a > 30 => println(s"$n isolder than 30")
      case Person(n, a, _) if a <= 30 => println(s"$n is a younger ")
    }
    
    println( "Factorial is :" + fact(3))
    
  }

}