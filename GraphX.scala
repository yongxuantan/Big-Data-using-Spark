import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

object GraphX {

  def main(args: Array[String]): Unit= {

    val inpath = args(0)
    //val inpath = "/Users/Mike/Documents/Summer2019/Assign3/wiki-Vote.txt"
    val outpath = args(1)
    //val outpath = "/Users/Mike/Documents/Summer2019/Assign3/graphxOutput"

    // create spark context
    val conf = new SparkConf().setAppName("Assignment3")
    val sc = new SparkContext(conf)

    // create string holder to collect output values
    var str = ""

    // bring in source dataset, filtered out leading text
    val input = sc.textFile(inpath)
      .filter(!_.contains("#")).map(row => row.split("\t"))

    // create vertex from inlink
    val vertex = input.map(row => (row(0).toLong, row(0))).distinct()

    // create edges from inlink, outlink, count
    val edge = input.map(row => Edge(row(0).toLong, row(1).toLong, 1))

    val graph = Graph(vertex, edge).persist()

    str=str+"\n"+"top 5 nodes with the highest outdegree and find the count of the number of outgoing edges in each"
    graph.outDegrees.sortBy(_._2, false).take(5)
      .foreach(x => str=str+"\n\t"+"NodeID "+x._1+" has "+x._2+" outlinks.")
    str=str+"\n"

    str=str+"\n"+"top 5 nodes with the highest indegree and find the count of the number of incoming edges in each"
    graph.inDegrees.sortBy(_._2, false).take(5)
      .foreach(x => str=str+"\n\t"+"NodeID "+x._1+" has "+x._2+" inlinks.")
    str=str+"\n"

    str=str+"\n"+"Calculate PageRank for each of the nodes and output the top 5 nodes with the highest PageRank values. You are free to define the threshold parameter."
    val ranks = graph.pageRank(0.1).vertices
    ranks.sortBy(_._2,false).take(5)
      .foreach(x => str=str+"\n\t"+"NodeID "+x._1+" has rank "+x._2+";")
    str=str+"\n"

    str=str+"\n"+"Run the connected components algorithm on it and find the top 5 components with the largest number of nodes."
    val cc = graph.connectedComponents().vertices
    cc.sortBy(_._2, false).take(5)
      .foreach(x => str=str+"\n\t"+"NodeID "+x._1+" has "+x._2+" number of nodes;")
    str=str+"\n"

    str=str+"\n"+"Run the triangle counts algorithm on each of the vertices and output the top 5 vertices with the largest triangle count. In case of ties, you can randomly select the top 5 vertices."
    val triCounts = graph.triangleCount().vertices
    triCounts.sortBy(_._2, false).take(5)
      .foreach(x => str=str+"\n\t"+"NodeID "+x._1+" has triangle count "+x._2+";")
    str=str+"\n"

    // output string
    print(str)
    sc.parallelize(Seq(str))
      .saveAsTextFile(outpath)
  }
}
