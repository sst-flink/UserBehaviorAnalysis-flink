package com.atguigu.LoginFailDetect

import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case  class  LoginEventT( userId: Long, ip: String, eventType: String, eventTime: Long)
object LoginFailCopy {
//  隐士转换
//  import org.apache.flink.streaming.api.scala._
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    env.fromCollection(Array(
      LoginEventT(1, "192.168.0.1", "fail", 1558430842),
      LoginEventT(1, "192.168.0.2", "fail", 1558430843),
      LoginEventT(1, "192.168.0.3", "fail", 1558430844),
      LoginEventT(2, "192.168.10.10", "success", 1558430845))).assignAscendingTimestamps(_.eventTime*1000)
      .filter(_.eventType=="fail")
      .keyBy(_.userId)
      .process(new MatchFunctionT())
      .print()
  env.execute("start")
  }
  class MatchFunctionT  extends  KeyedProcessFunction[Long,LoginEventT,LoginEventT]{
    lazy  val failState = getRuntimeContext.getListState(new ListStateDescriptor[LoginEventT]("state",classOf[LoginEventT]))
    override def processElement(value: LoginEventT, ctx: KeyedProcessFunction[Long, LoginEventT, LoginEventT]#Context, out: Collector[LoginEventT]): Unit = {
      failState.add(value)
      ctx.timerService().registerEventTimeTimer(value.eventTime+2*1000)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEventT, LoginEventT]#OnTimerContext, out: Collector[LoginEventT]): Unit = {
      val listBuffer:ListBuffer[LoginEventT]= new ListBuffer[LoginEventT]
      import  scala.collection.JavaConversions._
      for(login <- failState.get()){
        listBuffer+=login
      }
      failState.clear()

      if(listBuffer.length>1){
        out.collect(listBuffer.head)
      }

    }
  }
}
