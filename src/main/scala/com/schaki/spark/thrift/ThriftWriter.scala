package com.schaki.spark.thrift

import com.schaki.spark.playground.datatypes.Person
import com.twitter.elephantbird.mapreduce.io.ThriftWritable
import com.twitter.elephantbird.mapreduce.output.LzoThriftBlockOutputFormat
import com.twitter.elephantbird.util.{TypeRef, ThriftUtils, HadoopCompat}
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{PropertyConfigurator, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by sourabh.chaki on 3/11/16.
 */
object ThriftWriter {


  val log : Logger  = Logger.getRootLogger

  def createPersons(): List[Person] = {
    List(new Person("id1","name1"),new Person("id2","name2"))
  }

  def main(args: Array[String]) {


    PropertyConfigurator.configure("src/main/resources/log4j.properties")
    log.info("this is a test log")
    val conf = new SparkConf()

    conf.setAppName("ThriftTest")
    conf.setMaster("local")
    conf.registerKryoClasses(Array(classOf[LongWritable],classOf[Text]))
   // conf.set("io.compression.codecs","com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec")


    val sc = new SparkContext(conf)

    val persons : List[Person] = createPersons()

    println(sc.parallelize(persons).count())

    val job = new Job()

    val hadoopConf = HadoopCompat.getConfiguration(job)
  //  hadoopConf.set("elephantbird.class.for.MultiInputFormat", classOf[Person].getName)
   // hadoopConf.set("io.compression.codec.lzo.class","com.hadoop.compression.lzo.LzoCodec")
   // hadoopConf.set("io.compression.codecs","com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec")

    LzoThriftBlockOutputFormat.setClassConf(classOf[Person], hadoopConf);
    job.setOutputFormatClass(classOf[LzoThriftBlockOutputFormat[Person]]);


    val rdd : RDD[(Void,ThriftWritable[Person])] = sc.parallelize(persons).map(obj=>(null,new ThriftWritable[Person](obj, ThriftUtils.getTypeRef(classOf[Person]).asInstanceOf[TypeRef[Person]])))
    rdd.saveAsNewAPIHadoopFile(
      "out/person.lzo",
      classOf[Void],
      classOf[ThriftWritable[Person]],
      classOf[LzoThriftBlockOutputFormat[Person]],
      hadoopConf
    )


  }


}
