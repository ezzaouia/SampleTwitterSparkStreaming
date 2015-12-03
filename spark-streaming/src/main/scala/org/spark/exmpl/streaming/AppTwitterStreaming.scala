package org.spark.exmpl.streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.hadoop.conf.Configuration
import org.bson.BasicBSONObject
import twitter4j.GeoLocation
import org.apache.commons.lang3.builder.EqualsBuilder
import org.apache.commons.lang3.builder.HashCodeBuilder
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.Authorization
import twitter4j.TwitterFactory

/**
 * @author : ezzaouia.mohamed@gmail.com
 *
 */

object AppTwitterStreaming {

  def main(args: Array[String]) {

    val checkpointDir = sys.env.getOrElse("CHECKPOINT_DIR", "/tmp").toString

    // Mongodb over hadoop configuration
    val config = new Configuration()
    
    // mongodb collection must be capped in order 
    // to use mongodb's tailable cursor feature to stream data to web client
    // cmd to create a such collection : db.createCollection( "tweets", { capped: true, size: 100000 } )
    config.set("mongo.output.uri", "mongodb://127.0.0.1:27017/appTwitterStream.tweets")

    // Set the twitter properties to generate OAuth credentials
    val confgbuilder = new ConfigurationBuilder()
      .setDebugEnabled(true)
      .setOAuthConsumerKey("CIj0W2hOxXKjH9GjaHrHqCceO")
      .setOAuthConsumerSecret("TjQbNucsfnVo8kufSWtYSomfzjWsr4f05hhjB8dGBGWKVdDW9P")
      .setOAuthAccessToken("1491121548-rkbgplMNZUxViVX9VY43chOkPBwBPpVcLlR1v01")
      .setOAuthAccessTokenSecret("I4Bo0EqMcLOB0wrPJba6RdcKpWO4w12TWyzZnfdV53iBh")
      .build()

    val twitterFactory = new TwitterFactory(confgbuilder)
    val twitterOAth = new twitter4j.auth.OAuthAuthorization(confgbuilder)
    val twitterAuth: Option[twitter4j.auth.Authorization] = Some(twitterFactory.getInstance(twitterOAth).getAuthorization())

    // spark conf
    val conf = new SparkConf().setMaster("local[*]").setAppName("SampleTwitterStreaming")

    // spark streaming by slide of 2 seconds
    val ssc = new StreamingContext(conf, Seconds(2))

    // check point directory as failure backup for spark streaming 
    ssc.checkpoint(checkpointDir)

    // filters used to filter twitter status
    val filters = List("cat", "happy")
    val stream = TwitterUtils.createStream(ssc, twitterAuth, filters)

    // a class case as Tweet object
    case class Tweet(text: String, geoLoc: GeoLocation) {

      override def equals(o: Any) = o match {
        case that: Tweet => that.text.equalsIgnoreCase(this.text)
        case _           => false
      }

      override def hashCode() = this.text.toUpperCase.hashCode

      override def toString(): String = {
        "[tex: " + text + " , " + geoLoc + "]"
      }
    }

    // only status which contains geoLoc info
    val streamWithGeoLoc = stream.filter(_.getGeoLocation() != null)

    // status => (status.text, status.geoLoc)
    val statusGeoLoc = streamWithGeoLoc.map(status => (status.getText(), status.getGeoLocation))

    // (status.text, status.geoLoc) => ([ hashtags in status.text ], status.geoLoc)
    val wordsHashtagsGeoLoc = statusGeoLoc.map(tweet => (tweet._1.split(" ").filter { _.startsWith("#") }, tweet._2))

    // get red of newline ..
    val hashtagsGeoLoc = wordsHashtagsGeoLoc.flatMap(t => t._1.map { x => (x.replaceAll("[\\r\\n]", " "), t._2) })

    // => get Tweet object as stream
    val tweets = hashtagsGeoLoc.map(t => Tweet(t._1, t._2))

    // count hashtags by window
    val counts = tweets.map(tweet => (tweet, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(10), Seconds(2))

    // sorting by count
    val sortedCounts = counts.map { case (tag, count) => (count, tag) }
      .transform(rdd => rdd.sortByKey(false))

    // get top ten elements
    val top10 = sortedCounts.repartition(1).mapPartitions(_.take(10))

    // DStream as BSON object to save in mongodb
    val saveDStream = top10.map((tpl) => {
      val bson = new BasicBSONObject()
      bson.put("count", tpl._1.toString())
      bson.put("htag", tpl._2.text)
      bson.put("latitude", tpl._2.geoLoc.getLatitude.toString())
      bson.put("longitude", tpl._2.geoLoc.getLongitude.toString())
      (null, bson)
    })

    /** save stream as mongodb docuement
     *  
      { _id: 565b7648fb8f6868fbb5918d,
        count: '1',
        htag: '#vscocam',
        latitude: '40.75082967',
        longitude: '-73.82719803' 
      }
     */
    saveDStream.saveAsNewAPIHadoopFiles("", "", classOf[Any], classOf[Any], classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], config)

    //    streamWithGeoLoc.foreachRDD(rdd => {
    //      val ten = rdd.take(10)
    //      if (ten.size > 0) {
    //        println("\nTop 10 hashtags $$$:")
    //        val count = rdd.take(10).mkString("\n")
    //        println(count)
    //      }
    //    })
    //    
    //    sortedCounts.foreachRDD(rdd => {
    //      val ten = rdd.take(10)
    //      if (ten.size > 0) {
    //        println("\nTop 10 hashtags ***:")
    //        val count = rdd.take(10).mkString("\n")
    //        println(count)
    //      }
    //    })
    //    
    //    tweets.foreachRDD(rdd => {
    //      val ten = rdd.take(10)
    //      if (ten.size > 0) {
    //        println("\nTop 10 hashtags *tt*:")
    //        val count = rdd.take(10).mkString("\n")
    //        println(count)
    //      }
    //    })

    ssc.start()
    ssc.awaitTermination()

  }
}