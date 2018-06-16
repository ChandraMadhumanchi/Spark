package com.scala.practice

object Pattern {
  
  val n = 4
  
  n match {
    
    case 1 | 3 | 5 => println("It is odd")
    case 2 | 4 | 6 => println("It is even")
    case _ => println("Something else")
  }
  
  def doIt(x: Any) = x match {
    case _: Int => println("It's an Int")
    case _: String => println("It's String")
    case _ => println("It's something else")
  }
  
  def doIt2(x: Any) = x match {
    case n: Int => println(s"It's an Int, value is $n")
    case s: String => println(s"It's a string $s ")
    case _ => println("It's something else")
  }
  
  def doIt3(a: Int, b:Int) = (a,b) match {
    case (1,1) => println("1,1")
    case (1,_) => println("1, _")
    case (_,2) => println("_,2")
    case (_,_) => println("default")
  }
  def main(args : Array[String]){
    //doIt(n)
    //doIt("abc")
    //doIt(32.0)
    
    //doIt2(n)
    //doIt2("Chandra")
    doIt3(1,1)
    doIt3(3,2)
    
  }
}