package com.alvsanand.spark.lsh.core

import org.apache.commons.math3.primes.Primes
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import scala.reflect.ClassTag
import scala.util.Random

case class IndexedArray(index: Int, elems: Array[String])

class LSH(private var rows: Int, private var bands: Int, private var shingleLength: Int) extends Serializable {
  var model: scala.collection.Map[(Int, Int), List[Int]] = null

  var dictionary: scala.collection.Map[String, Int] = null
  var minHashFunctions: Array[MinHashFunction] = null
  var bandHashFunctions: IndexedSeq[BandHashFunction] = null

  def generateMinHashFunctions(universeSize: Int): Array[MinHashFunction] = {

    /**
     * create a valid prime list by excluding prime factors of universeSize
     * make sure that each minhash function has a good distribution
     */

    val primeNumber = Primes.nextPrime(universeSize)

    Array.tabulate(bands * rows) {
      _ =>
        MinHashFunction(
          a = Random.nextInt(primeNumber),
          b = Random.nextInt(universeSize),
          primeNumber)
    }
  }

  def generateBandHashFunctions() =
    for {
      i <- 0 until bands
    } yield BandHashFunction(Random.nextInt)

  def run(sc: SparkContext, itemSets: RDD[IndexedArray]) = {
    dictionary = itemSets.flatMap(x => x.elems.toList.sliding(shingleLength).map(_.mkString))
      .distinct
      .zipWithIndex
      //.sortBy(identity) // Sort may be skipped for performance issue
      .collect
      .map(x => (x._1, x._2.toInt)).toMap

    val bdcUniversalSet = sc.broadcast(dictionary)

    minHashFunctions = generateMinHashFunctions(dictionary.size)
    val bdcMinHashFunctions = sc.broadcast(minHashFunctions)

    bandHashFunctions = generateBandHashFunctions
    val bdcBandHashFunctions = sc.broadcast(bandHashFunctions)

    model = itemSets.flatMap {
      case IndexedArray(id, elems) => {
        val universalSet = bdcUniversalSet.value
        val minHashFuncs = bdcMinHashFunctions.value
        val bandHashFuncs = bdcBandHashFunctions.value

        val shingles = elems.toList.sliding(shingleLength).map(_.mkString).toSet

        val shinglesIndexInUniversalSet = shingles.map(p => universalSet(p))

        val signatures = for {
          hash <- minHashFuncs
        } yield (shinglesIndexInUniversalSet map hash.apply).min

        signatures.grouped(rows).zipWithIndex
          .map {
            case (arr, bandId) =>
              val bucketId = bandHashFuncs(bandId)(arr)
              (bandId, bucketId, id)
          }
      }
    }.groupBy {
      case (bandId, bucketId, _) => (bandId, bucketId)
    }.map(v => (v._1, v._2.map(x => x._3).toList)).filter(_._2.size > 1).collectAsMap

    this
  }

  def similarsTo(array: IndexedArray, threshold: Double) = {
    val id = array.index
    val elems = array.elems

    val shingles = elems.toList.sliding(shingleLength).map(_.mkString).toSet

//    println("Sentence[%s] has these shingles: %s".format(array.elems.mkString, shingles.toList.mkString(" && ")))

    val shinglesIndexInUniversalSet = shingles.map(p => dictionary(p))

    val signatures = for {
      hash <- minHashFunctions
    } yield (shinglesIndexInUniversalSet map hash.apply).min

    val bands = signatures.grouped(rows).zipWithIndex
      .map {
        case (arr, bandId) =>
          val bucketId = bandHashFunctions(bandId)(arr)
          (bandId, bucketId)
      }

    val minCommonBuckets = rows * (1 - threshold)

    bands.filter(x => model.contains(x))
      .flatMap(x => model(x).map((_, 1))).toList
      .groupBy(_._1).mapValues(x => x.size)
      .filter(x => id != x._1 && x._2 >= minCommonBuckets).toSet
  }
}