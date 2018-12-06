import java.io.{BufferedReader, InputStreamReader, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.lib.ShortestPaths

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
//import scala.io.Codec



object mf1832107 {
  def exchangeedge(x:(Edge[Int])): (Edge[Int])={
    val a = x.srcId
    x.srcId = x.dstId
    x.dstId = a
    x
  }

  def main(args:Array[String]): Unit ={
    //定义边角数组
    var vertexArr=new ArrayBuffer[(Long,String)]()
    var edgeArr=new ArrayBuffer[Edge[Int]]()
    //  控制台输入，并给出两文件地址
    val br = new BufferedReader(new InputStreamReader(System.in));
    val path = br.readLine()

    val datapath=path+"data.txt"
    //  将data内容存入array
    val datafile=Source.fromFile(datapath)
    for (line<-datafile.getLines())
    {
      //      println(line)
      val result=line.split(',')
      val startvertex=result(0).substring(2)
      val endvertex=result(1).substring(2)
      vertexArr+=((startvertex.toLong,startvertex))
      vertexArr+=((endvertex.toLong,endvertex))
      edgeArr+=Edge(startvertex.toLong,endvertex.toLong,1)
    }
    println(vertexArr)
    println(edgeArr)
    datafile.close()
    //读variable文件
    val variablespath=path+"variables.txt"
    val variablesfile=Source.fromFile(variablespath)
    val line=variablesfile.getLines().toArray
    //  println(line)
    //  println(line.getClass.getSimpleName)
    val FirstNode=line(0).substring(2)
    val SecondNode=line(1).substring(2)
    variablesfile.close()
    //  println(FirstNode)
    //开启spark服务
    var num2=0
    val conf = new SparkConf().setAppName("Graphx").setMaster("local")
    val sc = new SparkContext(conf)
      Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //  array转化为rdd,构建图和合并有序集合之后的图
    val vertexRDDOri:RDD[(VertexId,String)]=sc.parallelize(vertexArr)
    val vertexRDDDis:RDD[(VertexId,String)]=vertexRDDOri.distinct()

    val edgeRDDOri:RDD[Edge[Int]]=sc.parallelize(edgeArr)
    val edgeRDDDis:RDD[Edge[Int]]=edgeRDDOri.distinct()

    val graghOri=Graph(vertexRDDOri,edgeRDDOri)
    val graphDis=Graph(vertexRDDDis,edgeRDDDis)
    //构造反向边集合
    val exchangeEdge:RDD[Edge[Int]]=edgeRDDDis.map(x=>exchangeedge(x))

    //  计算双向边
    val bothwayEdge:RDD[Edge[Int]]=exchangeEdge.intersection(edgeRDDDis)
    println(bothwayEdge.collect())
    var num1=(bothwayEdge.collect().length)/2
    println(num1)
    //  计算重复边

    val SortEdge=graghOri.edges.groupBy(x=>(x.srcId,x.dstId))
    SortEdge.collect().foreach({x=>if(x._2.size > 1){num2+=1}})
    println(num2)

    //  计算最短路径
    val Gragh1=Graph(vertexRDDDis,edgeRDDDis).cache()
    val Graph2=ShortestPaths.run(Gragh1,Seq(FirstNode.toLong))
    val FindNode=Graph2.vertices.filter(x=>x._1==SecondNode.toLong)
    val num3=FindNode.collect()(0)._2.values.toString().substring(8,9)
    println(num3)
    val output=new PrintWriter(path+"mf1832107.txt")
    output.write(num1.toString)
    output.write("\r\n")
    output.write(num2.toString)
    output.write("\r\n")
    output.write(num3.toString)
    output.close()
  }
}

