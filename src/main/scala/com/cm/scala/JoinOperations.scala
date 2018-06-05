package com.cm.scala

import scala.collection.immutable.List
import scala.collection.mutable.HashMap
import scala.io.Source
import java.io.FileNotFoundException
import java.io.IOException
import scala.util.control.Exception
/*
 *   - Problem Statement : implement the logic of a **left join** in given 2 data sets
 * 	 - Implement a left join in pure scala environment 
 *   - Do not use language built-in functions for joining data
 * 	 - Do not use external libraries
 *   - Do not use SQL
 */

object JoinOperations {

  // model class for employee_names.scv
  case class Employee(id: String, first_name: String, last_name: String)

  // model class for employee_pay.scv
  case class Pay(id: String, salary: String, bonus: String)

  def main(args: Array[String]) {

    if (args.length < 1) {
      System.err.println("Usage: scala RunAverager <fullpath>/")
      System.exit(1)
    }
    // employee_names.csv
    val employeeNamesfilename = args(0)
    // employee_pay.csv
    val employeePayfilename = args(1)
    try {
      val employees: List[Employee] = getEmployees(employeeNamesfilename)

      // make a map for O(1) pay lookups from employee Id
      // we're assuming all entries are unique

      val employeePayMap = getEmployeesPay(employeePayfilename)

      // if no employee is found in map, we're using 'null'
      // end up with a list of out put(id, first_name, last_name,  salary, bonus)

      employees.foreach { x =>
        if (employeePayMap.contains(x.id)) {
          val payRecord = employeePayMap.get(x.id).get
          println(s"${x.id},${x.first_name},${x.last_name},${payRecord._1},${payRecord._2}")
        } else {
          println(s"${x.id},${x.first_name},${x.last_name},${null},${null}")
        }
      }
    } catch {
      case ex: FileNotFoundException => {
        println("Missing file exception")
      }
      case ex: IOException => {
        println("IO Exception")
      }
      case _: Throwable => println("Exception caught")
    }
  }

  /**
   * reads a data file containing formatted data into a list containing Employee case class.
   *
   * @param fileName name of the file (with extension) to read the data from.
   * @return a list of case classes containing the data from the specified file.
   * id: String, first_name: String, last_name: String
   *
   */
  def getEmployees(fileName: String): List[Employee] = {
    val employeeNames: List[Employee] = scala.io.Source.fromFile(fileName).getLines().toList map { line =>
      line.trim.split(",", -1) match {
        case Array(id, first_name, last_name) => Employee(id, first_name, last_name)
      }
    }
    employeeNames
  }

  /**
   * reads a data file containing formatted data into a list containing Employee case class.
   *
   * @param fileName name of the file (with extension) to read the data from.
   * @return a list of case classes containing the data from the specified file.
   * id: String, salary: String, bonus: String
   *
   */
  def getPay(fileName: String): List[Pay] = {
    val employeePay: List[Pay] = scala.io.Source.fromFile(fileName).getLines().toList map { line =>
      line.trim.split(",", -1) match {
        case Array(id, salary, bonus) => Pay(id, salary, bonus)
      }
    }
    employeePay
  }

  /**
   * reads a data file containing formatted data into a map.
   *
   * @param fileName name of the file (with extension) to read the data from.
   * @return a map data from the specified file.
   * id: String, salary: String, bonus: String
   *
   */
  def getEmployeesPay(filename: String): HashMap[String, (String, String)] = {
    val employee_payLines = scala.io.Source.fromFile(filename).getLines
    var employee_payMap = new HashMap[String, (String, String)]()
    for (i <- employee_payLines) {
      var temp = i.split(",", -1)
      employee_payMap += temp(0) -> (temp(1), temp(2))
    }
    employee_payMap
  }

  /**
   * Output
   * id,first_name,last_name,salary,bonus
   * 59ea7840fc13ae1f6d000096,Benny,Ventom,$84217.20,
   * 59ea7840fc13ae1f6d000097,Cara,Motherwell,$54737.84,
   * 59ea7840fc13ae1f6d000098,Willem,Haresign,$92092.21,
   * 59ea7840fc13ae1f6d000099,Trish,Farlane,$86117.63,
   * 59ea7840fc13ae1f6d00009a,Camilla,Limpkin,$92821.29,
   * 59ea7840fc13ae1f6d00009b,Godwin,Caffrey,$60633.55,$1779.07
   * 59ea7840fc13ae1f6d00009c,Elvera,Custed,$94483.80,$4328.59
   * 59ea7840fc13ae1f6d00009d,Vinson,Farres,$93127.77,
   * 59ea7840fc13ae1f6d00009e,Kliment,Pitchford,$90581.06,
   * 59ea7840fc13ae1f6d00009f,Matty,Heater,$86947.54,
   * 59ea7840fc13ae1f6d0000a0,Juana,Begg,$80646.00,$2924.69
   * 59ea7840fc13ae1f6d0000a1,Alix,Layus,$50423.64,$1495.33
   * 59ea7840fc13ae1f6d0000a2,Tessa,Brandes,$60695.47,
   * 59ea7840fc13ae1f6d0000a3,Hedwig,Fishley,$85763.22,
   * 59ea7840fc13ae1f6d0000a4,Cordelia,Aubray,$60600.37,
   */

}