package com.mlesniak.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Application entry point.
  *
  * @author Michael Lesniak (mail@mlesniak.com)
  */
object Main extends App {
    // We use 64 partitions to have sufficient parallelization on local systems.
    val minPartitions: Int = 64

    val conf = new SparkConf()
        .setAppName("Music playground")
        .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    def computeStatistics() = {
        // Analyze range of user and artist ids.
        val userStats = rawUserArtist.map(_.split(' ')(0).toDouble).stats()
        val artistStats = rawUserArtist.map(_.split(' ')(1).toDouble).stats()

        println("*** Statistics")
        println(userStats)
        println(artistStats)
    }


    val userDir = System.getProperty("user.dir")
    val rawUserArtist = sc.textFile(s"file://$userDir/data/user_artist_data.txt", minPartitions)
    rawUserArtist.persist()

    val rawArtist = sc.textFile(s"file://$userDir/data/artist_data.txt", minPartitions)
    var artistByID = rawArtist.flatMap { line =>
        val (id, name) = line.span(_ != '\t')
        if (name.isEmpty) {
            None
        } else {
            try {
                Some((id.toInt, name.trim))
            } catch {
                case e: NumberFormatException => None
            }
        }
    }
    artistByID.persist()
    println(artistByID.count())
    artistByID.take(10).foreach(println)
}
