package com.alvsanand.spark.lsh.core

import org.junit.runner.RunWith
import org.specs2.mutable._
import org.specs2.runner._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

@RunWith(classOf[JUnitRunner])
class LSHTest extends Specification {
  val conf = new SparkConf()
    .setAppName("The swankiest Spark app ever")
    .setMaster("local[2]")

  val sc = new SparkContext(conf)

  "The 'Hello world' string" should {

    val createIndexedSetFromLine = (line: String) => {
      IndexedArray(line.trim.split(' ').head.toInt, line.trim.split(' ').tail)
    };

    val data = sc.textFile(getClass.getResource("/sentences_small.txt").getFile)

    val sentences: org.apache.spark.rdd.RDD[IndexedArray] = data.map(createIndexedSetFromLine)

    sentences.collect.foreach(x => println(x.index + "=> " + x.elems.mkString(" ")))

    val sentencesLengths: scala.collection.Map[Int, Int] = sentences.map(x => (x.index, x.elems.size)).collectAsMap

    val rows = 3
    val bands = 10
    val shingleLength = 2


    val lsh = new LSH(bands, rows, shingleLength)
      .run(sc, sentences)

    println(lsh.model)

    for (sen <- sentences.collect()) {
      val t = Math.pow(1/bands, (1/rows))

      val candidates = lsh.similarsTo(sen, t)

      if (candidates.size > 0) {
        println("Sentence[%d] has similar the following similar cadidates: %s".format(sen.index, candidates.mkString(" && ")))
      }
    }

    //    "end with 'world'" in {
    //      "Hello world" must endWith("world")
    //    }
  }
}
