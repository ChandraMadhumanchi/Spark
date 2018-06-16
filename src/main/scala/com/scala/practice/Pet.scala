package com.scala.practice

import java.time.LocalDate
import java.time.LocalTime

class Pet {
  def feed() = {
    "Feeding at: "+ LocalTime.now()
  }
}

class Cat extends Pet {
  def hunt() = {
    println("The cat hunts")
  }
}

