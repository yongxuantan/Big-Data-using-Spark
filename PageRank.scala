import org.apache.spark.{SparkConf, SparkContext} 

object PageRank {
  def main(args: Array[String]): Unit = {

    val inpath = args(0)
    val maxiter = args(1)
    val outpath = args(2)

    var probA = 0.15               // probability of random jump
    var initialValue = 10.0        // initial page rank value

    println(inpath)
    println(maxiter)
    println(outpath)

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)

    // bring data into an RDD
    val input = sc.textFile(inpath)

    // use to remove header from input
    var header = input.first()

    // create an RDD with ORIGIN as key and DEST as value
    var outlink = input.filter(row => row != header).map{line => {
      var col = line.split(",")
      (col(0), col(1))
    }}

    // create N variable to be the total number of links in graph, treat multiple connections as independent links
    var Nlinks = outlink.count()

    // group all outlink by its ORIGIN as key and list of DEST as value
    var links = outlink.groupByKey()

    // create page rank with initual value
    var pageRank = links.mapValues(v => initialValue)

    // run up to maxiter number of times
    for (i <- 1 to maxiter.toInt) {

      // calculate the second part of the equation; it is the contributions to this page rank value
      // 1. combine links with existing page rank to get an RDD with ORIGIN as key and values ([list of DEST], initial PR value)
      // 2. only keeping values from step 1, so RDD now has ([list of DEST], initial PR value)
      // 3. for each value from step 2:
      //      a. get the number of outlinks,
      //      b. set each share to be initial PR value divided by number of outlinks
      //      c. for each DEST from list: map DEST as key and share as value
      // 4. flat maps all K,V pairs from step 3.c
      var contributions = links.join(pageRank).values.flatMap{case (dests, rank) =>
        var outdegree = dests.size
        var share = rank/outdegree
        dests.map(dest => (dest, share))
      }

      // calculate the new page rank value as: a / N + (1-a) * sum(contributions)
      // 1. sum up contributions by key to get the total amount of contributions each DEST get from their inlinks
      // 2. map according to equation
      pageRank = contributions.reduceByKey(_ + _).mapValues(probA / Nlinks  + (1 - probA) * _)

    }

    // save final page rank
    pageRank.sortBy(-_._2).saveAsTextFile(outpath)

  }
}
