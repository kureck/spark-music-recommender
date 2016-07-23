package com.mlesniak.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Application entry point.
  *
  * @author Michael Lesniak (mail@mlesniak.com)
  */
object Main extends App {
    val conf = new SparkConf()
        .setAppName("Music playground")
        .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val passwd: RDD[String] = sc.textFile("/etc/passwd")
    val count = passwd.count()

    println(count)
}
