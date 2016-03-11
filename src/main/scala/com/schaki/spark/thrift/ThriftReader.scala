package com.schaki.spark.thrift

import com.schaki.spark.playground.datatypes.Person
import com.twitter.elephantbird.mapreduce.input.{LzoThriftBlockInputFormat, MultiInputFormat}
import com.twitter.elephantbird.mapreduce.io.BinaryWritable
import com.twitter.elephantbird.mapreduce.output.LzoThriftBlockOutputFormat
import com.twitter.elephantbird.util.HadoopCompat
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{PropertyConfigurator, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by sourabh.chaki on 3/11/16.
 */
object ThriftReader {

  val log : Logger  = Logger.getRootLogger

  def main(args: Array[String]) {


    PropertyConfigurator.configure("src/main/resources/log4j.properties")
    log.info("this is a test log")
    val conf = new SparkConf()

    conf.setAppName("ThriftTest")
    conf.setMaster("local")
    conf.registerKryoClasses(Array(classOf[LongWritable],classOf[Text]))


    val sc = new SparkContext(conf)

    val job = new Job()

    val hadoopConf = HadoopCompat.getConfiguration(job)
    hadoopConf.set("io.compression.codecs","com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec")


    LzoThriftBlockOutputFormat.setClassConf(classOf[Person], hadoopConf);
    MultiInputFormat.setClassConf(classOf[Person], hadoopConf)
    job.setInputFormatClass(classOf[LzoThriftBlockInputFormat[Person]]);

    val rdd :RDD[(LongWritable, BinaryWritable[Person])]  = sc.newAPIHadoopFile(
      "out/person.lzo",
      classOf[LzoThriftBlockInputFormat[Person]],
      classOf[LongWritable],
      classOf[BinaryWritable[Person]],
      hadoopConf)

    val persons = rdd.map(t=>(t._2.get()))
    persons.collect().map(println(_))

  }

}
