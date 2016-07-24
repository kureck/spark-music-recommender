package com.mlesniak.spark

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

    def computeStatistics() = {
        // We use 64 partitions to have sufficient parallelization on local systems.
        val userDir = System.getProperty("user.dir")
        var rawUserArtist = sc.textFile(s"file://${userDir}/data/user_artist_data.txt", 64)
        rawUserArtist.persist()

        // Analyze range of user and artist ids.
        val userStats = rawUserArtist.map(_.split(' ')(0).toDouble).stats()
        val artistStats = rawUserArtist.map(_.split(' ')(1).toDouble).stats()

        println("*** Statistics")
        println(userStats)
        println(artistStats)
    }

    computeStatistics()
}
