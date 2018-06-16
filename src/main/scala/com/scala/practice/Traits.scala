package com.scala.practice

trait Traits {
  def launch
}

trait Boat extends Traits {
  def launch = println("I'm a boat")
}

trait Plane extends Traits {
  def launch = println("I am in plain")
}




