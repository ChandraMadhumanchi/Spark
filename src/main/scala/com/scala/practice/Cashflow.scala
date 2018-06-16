package com.scala.practice

import java.time.LocalDateTime

class Cashflow(val amount: Double, val currency: String, val due: LocalDateTime) {
  
  def this(amount : Double, due: LocalDateTime) = this(amount, "USD", due)
  
  def this(amount : Double) = this(amount, LocalDateTime.now())
  
  val settle = due.toLocalDate.plusDays(2)
  
  private lazy val processAt = LocalDateTime.now
  
  def rollForward() = {
    
    val retval = new Cashflow(amount,currency,due.plusDays(1))
    
    retval.processAt
    retval
    
  }
  
}

object Cashflow {
  def main(args: Array[String]) = {
    val c1 = new Cashflow(100.0)
    println(c1.settle)
    
    val c2 = c1.rollForward()
    Thread.sleep(1000)
    println(c2.settle)
    println(c1.processAt)
    println(c2.processAt)
  }
}