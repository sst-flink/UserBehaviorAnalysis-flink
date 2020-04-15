package com.atguigu.LoginFailDetect

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.LoginFailDetect
  * Version: 1.0
  *
  * Created by wushengran on 2019/6/14 9:12
  */
object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val loginStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.0.3", "fail", 1558430845),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.userId)

    // 定义一个匹配模式，next紧邻发生的事件
    val loginFailPattern = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .times(2)
      .within(Time.seconds(3))

    // 在keyby之后的流中匹配出定义好的pattern stream
    val patternStream = CEP.pattern( loginStream, loginFailPattern )

    import scala.collection.Map
    // 从pattern stream 中获取匹配到的事件流
    val loginFailDataStream = patternStream.select(
      ( pattern: Map[String, Iterable[LoginEvent]] ) =>{
        val begin = pattern.getOrElse("begin", null).iterator.next()
        val next = pattern.getOrElse("next", null).iterator.next()
        (next.userId, begin.ip, next.ip, next.eventType)
      }
    ).print()
    print("====================")
    env.execute("Login Fail Detect Job")
  }

}
