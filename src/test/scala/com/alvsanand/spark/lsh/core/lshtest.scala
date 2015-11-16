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

    val lsh = new LSH(10, 10, 2)
      .run(sc, sentences)

    println(lsh.model)

    for (sen <- sentences.collect()) {
      val j1 = sen.elems.size / (sen.elems.size + 1).toDouble
      val j2 = (sen.elems.size - 1) / sen.elems.size.toDouble

      val sim1 = lsh.compute(sen, j1, 1)
      val sim2 = lsh.compute(sen, j2, 1)

      var s = scala.collection.mutable.Set[Int]()

      sim1.keys.filter(x => sen.elems.size >= sentencesLengths(x)).foreach(v => s += v)
      sim2.keys.filter(x => sen.elems.size <= sentencesLengths(x)).foreach(v => s += v)

      if (s.size > 0) {
        println("Sentence[%d] is similar to: %s".format(sen.index, s.mkString(" && ")))
      }
    }

    //    "end with 'world'" in {
    //      "Hello world" must endWith("world")
    //    }
  }
}
