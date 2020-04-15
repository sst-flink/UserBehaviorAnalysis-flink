package com.atguigu.NetWorkTrafficAnalysis

import java.text.SimpleDateFormat

import com.atguigu.NetWorkTrafficAnalysis.NetworkTraffic.WindowResultFunction
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @Auther:sst
  * @Date:2020 /4/14
  * @Description:com.atguigu.NetWorkTrafficAnalysis
  * @version:1.0
  */
// 输入数据格式
case class ApacheLogEventT( ip: String, userId: String, eventTime: Long, method: String, url: String )
// 输出数据格式
case class UrlViewCountT( url: String, windowEnd: Long, count: Long )
object NetworkTrafficCopy {
  def main(args: Array[String]): Unit = {
    val env  = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val path =getClass.getClassLoader.getResource("apache.log").getPath
    val stream = env.readTextFile(path)
    stream.map(line => {
        val lines = line.split(" ")
        val dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val enentTime = dateFormat.parse(lines(3)).getTime
      ApacheLogEvent(lines(0),lines(2),enentTime,lines(5),lines(6))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1000)) {
      override def extractTimestamp(element: ApacheLogEvent): Long = {
          element.eventTime
      }
    }).filter(_.method=="GET")
      .keyBy(_.url)
      .timeWindow(Time.seconds(50),Time.seconds(5))
      .aggregate( new CountAggT(),new WindowResultFunctionT())
      .keyBy("windowEnd")
//      .process(new TopNHotUrlsT())
//      .print()
    env.execute("cacuteUrl")
  }
  class  CountAggT() extends  AggregateFunction[ApacheLogEvent,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: ApacheLogEvent, acc: Long): Long = acc+1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc+acc1
  }
  class  WindowResultFunctionT() extends  WindowFunction[Long,UrlViewCount,String,TimeWindow]{
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      val  url = key
      val count = input.iterator.next()
      out.collect(UrlViewCount(url,window.getEnd,count))
    }
  }
  class  TopNHotUrlsT(topSize:Int)  extends  KeyedProcessFunction[]{

  }
}
