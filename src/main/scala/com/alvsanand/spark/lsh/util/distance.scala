package com.alvsanand.spark.lsh.util

abstract class Distance[T] {
  def calculateDistance(o1: T, o2: T): Int
}

//class LevenshteinDistance extends Distance[String] {
//
//  def minimum(i1: Int, i2: Int, i3: Int) = Math.min(Math.min(i1, i2), i3)
//
//  def calculateDistance(s1: String, s2: String): Int = {
//    val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) => if (j == 0) i else if (i == 0) j else 0 }
//
//    for (j <- 1 to s2.length; i <- 1 to s1.length)
//      dist(j)(i) = if (s2(j - 1) == s1(i - 1)) dist(j - 1)(i - 1)
//      else minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)
//
//    dist(s2.length)(s1.length)
//  }
//}

object EditDistance extends Distance[Array[String]] {

  def minimum(i1: Int, i2: Int, i3: Int) = Math.min(Math.min(i1, i2), i3)

  def calculateDistance(s1: Array[String], s2: Array[String]): Int = {
    val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) => if (j == 0) i else if (i == 0) j else 0 }

    for (j <- 1 to s2.length; i <- 1 to s1.length)
      dist(j)(i) = if (s2(j - 1) == s1(i - 1)) dist(j - 1)(i - 1)
      else minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)

    dist(s2.length)(s1.length)
  }
}