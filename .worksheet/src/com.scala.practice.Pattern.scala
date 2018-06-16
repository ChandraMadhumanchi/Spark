package com.scala.practice

object Pattern {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(88); 
  println("Welcome to the Scala worksheet");$skip(15); 
  
  val n = 2;System.out.println("""n  : Int = """ + $show(n ));$skip(69); 
  
  n match {

	case 2 => println("Help from patteren match")
	
  }}
}
