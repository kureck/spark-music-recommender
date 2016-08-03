package com.mlesniak.spark

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Application entry point.
  *
  * @author Michael Lesniak (mail@mlesniak.com)
  */
object Main extends App {
    // We use 8 partitions to have sufficient parallelization on local systems.
    val minPartitions: Int = 8

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
    println("rawUserArtists.count:" + rawUserArtist.count())

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

    val rawArtistAlias = sc.textFile(s"file://$userDir/data/artist_alias.txt", minPartitions)
    var artistAlias = rawArtistAlias.flatMap { line =>
        val tokens = line.split("\t")
        if (tokens(0).isEmpty) {
            None
        } else {
            Some((tokens(0).toInt, tokens(1).toInt))
        }
    }.collectAsMap()
    val bArtistAlias = sc.broadcast(artistAlias)


    def trainAndSave() = {
        val trainData = rawUserArtist.map { line =>
            val Array(userID, artistID, count) = line.split(" ").map(_.toInt)
            //        println(s"\nline=$line")
            //        println(s"Mapping $artistID ...")
            val id = bArtistAlias.value.getOrElse(artistID, artistID)
            val r = Rating(userID, id, count)
            //        println(s"Computed rating $r")
            r
        }.cache()

        println(s"Train data size ${trainData.count()}")

        println("Training model")
        val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)

        println("Saving model")
        model.save(sc, "music-model")
    }

    println("Loading model")
    val model: MatrixFactorizationModel = MatrixFactorizationModel.load(sc, "music-model")

    //val rdd = model.userFeatures.mapValues(_.mkString(", ")).first()
    //println(rdd)

    val exUser = rawUserArtist.map(_.split(' ')).filter({
        case Array(user, _, _) => user.toInt == 2093760
    })

    val exProducts = exUser.map {
        case Array(_, artist, _) => artist.toInt
    }.collect().toSet

    artistByID.filter {
        case (id, name) => exProducts.contains(id)
    }.values.collect().foreach(println)

    val recommendations = model.recommendProducts(2093760, 5)
    recommendations.foreach(println)

    val recommendedProductIDs = recommendations.map(_.product).toSet
    artistByID.filter { case (id, name) =>
        recommendedProductIDs.contains(id)
    }.values.collect().foreach(println)

}
