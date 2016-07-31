package com.mlesniak.spark

import org.apache.spark.mllib.recommendation.Rating
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

    val rawArtistAlias = sc.textFile(s"file://$userDir/data/artist_alias.txt", minPartitions)
    var artistAlias = rawArtistAlias.flatMap { line =>
        val tokens = line.split("\t")
        if (tokens(0).isEmpty) {
            None
        } else {
            Some((tokens(0).toInt, tokens(1).toInt))
        }
    }

    artistAlias.persist()

    val a1 = artistByID.lookup(1092764)
    val a2 = artistByID.lookup(1000311)
    println(a1)
    println(a2)

    // Opimize broadcasting for multiple nodes.
    val bArtistAlias = sc.broadcast(artistAlias)

    val trainData = rawUserArtist.map { line =>
        val Array(userID, artistID, count) = line.split(" ").map(_.toInt)
        val id = bArtistAlias.value.lookup(artistID)
        val finalID: Int = if (id.isEmpty) {
            artistID
        } else {
            id.head
        }
        Rating(userID, finalID, count)
    }.cache()
}
