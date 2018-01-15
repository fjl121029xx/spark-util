package com.kakfa.offset.test

object othtest {
  def main(args: Array[String]): Unit = {
  Array(Array(1,2),Array(3,4,5)).flatMap { a => if(a.length>2) a else None }
  .foreach(println)
  
    
  }
}