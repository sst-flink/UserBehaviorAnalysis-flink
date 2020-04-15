package com.atguigu.NetWorkTrafficAnalysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.atguigu.NetWorkTrafficAnalysis.NetworkTraffic.WindowResultFunction
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
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

import scala.collection.mutable.ListBuffer
//import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala._

/**
  * @Auther:sst
  * @Date:2020 /4/14
  * @Description:com.atguigu.NetWorkTrafficAnalysis
  * @version:1.0
  */
// 输入数据格式
case class ApacheLogEventT(ip: String, userId: String, eventTime: Long, method: String, url: String)

// 输出数据格式
case class UrlViewCountT(url: String, windowEnd: Long, count: Long)

object NetworkTrafficCopy {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val path = getClass.getClassLoader.getResource("apache.log").getPath
    val stream = env.readTextFile(path)
    stream.map(line => {
      val lines = line.split(" ")
      val dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val enentTime = dateFormat.parse(lines(3)).getTime
      ApacheLogEventT(lines(0), lines(2), enentTime, lines(5), lines(6))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEventT](Time.seconds(1000)) {
      override def extractTimestamp(element: ApacheLogEventT): Long = {
        element.eventTime
      }
    }).filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.seconds(50), Time.seconds(5))
      .aggregate(new CountAggT(), new WindowResultFunctionT())
      .keyBy(_.windowEnd)
          .process(new TopNHotUrlsT(3))
          .print()
    env.execute("cacuteUrl")
  }

  class CountAggT() extends AggregateFunction[ApacheLogEventT, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: ApacheLogEventT, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  class WindowResultFunctionT() extends WindowFunction[Long, UrlViewCountT, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCountT]): Unit = {
      val url = key
      val count = input.iterator.next()
      out.collect(UrlViewCountT(url, window.getEnd, count))
    }
  }

  class TopNHotUrlsT(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCountT, String] {
    lazy val urlState = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCountT]("state", classOf[UrlViewCountT]))

    override def processElement(i: UrlViewCountT, context: KeyedProcessFunction[Long, UrlViewCountT, String]#Context, collector: Collector[String]): Unit = {
      urlState.add(i)
      context.timerService().registerEventTimeTimer(i.windowEnd+10*1000)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCountT, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 从状态中获取所有的Url访问量
      val allUrlViews: ListBuffer[UrlViewCountT] = ListBuffer()
      import scala.collection.JavaConversions._
      for( urlView <- urlState.get() ){
        allUrlViews += urlView
      }
      // 清空state
      urlState.clear()
      // 按照访问量排序输出
      val sortedUrlViews = allUrlViews.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

      // 将排名信息格式化成 String, 便于打印
      var result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间: ").append(new Timestamp(timestamp - 10 * 1000)).append("\n")

      for (i <- sortedUrlViews.indices) {
        val currentUrlView: UrlViewCountT = sortedUrlViews(i)
        // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
        result.append("No").append(i+1).append(":")
          .append("  URL=").append(currentUrlView.url)
          .append("  流量=").append(currentUrlView.count).append("\n")
      }
      result.append("====================================\n\n")

      Thread.sleep(500)
      out.collect(result.toString())
    }
  }

}
