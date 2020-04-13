import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * @Auther:sst
  * @Date:2020 /4/12
  * @Description:
  * @version:1.0
  */
// 输入数据样例类
case  class   UserBehaviorT(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)
// 输出数据样例类
case class ItemViewCountT( itemId: Long, windowEnd: Long, count: Long )
object HotItemsCopy {
  def main(args: Array[String]): Unit = {
    //创建一个env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //显示的定义time的类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val path = getClass.getClassLoader.getResource("UserBehavior.csv").getPath
    val stream = env.readTextFile(path).map(line =>{
      val lines = line.split(",")
      UserBehaviorT(lines(0).toLong,lines(1).toLong,lines(2).toInt,lines(3),lines(4).toLong)
    })
    //指定时间戳和watermark   数据中的时间戳是升序的
        .assignAscendingTimestamps(_.timestamp*1000)
      .filter(_.behavior=="pv")
      .keyBy("itemId")
      .timeWindow(Time.minutes(60),Time.minutes(5))
      .aggregate(new CountAggT(),new WindowResultFunctionT())
      .keyBy("windowEnd")
      .process( new TopNHotItemsT(3))
      .print()
    env.execute("Hot Items Job")
  }

  class  CountAggT    extends  AggregateFunction[UserBehaviorT,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: UserBehaviorT, acc: Long): Long = acc+1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc+acc1
  }

  class  WindowResultFunctionT   extends  WindowFunction[Long, ItemViewCountT, Tuple, TimeWindow]{
    override def apply(key: Tuple, w: TimeWindow, aggregateResult: Iterable[Long], collector: Collector[ItemViewCountT]): Unit = {
      val itemId:Long = key.asInstanceOf[Tuple1[Long]].f0
      val count = aggregateResult.iterator.next()
      collector.collect(ItemViewCountT(itemId,w.getEnd,count))
    }
  }

  class  TopNHotItemsT(topSize:Int) extends  KeyedProcessFunction[Tuple, ItemViewCountT, String]{
      private  var  itemState:ListState[ItemViewCountT]=_
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      val itemStateDesc= new ListStateDescriptor[ItemViewCountT]("itemcount",classOf[ItemViewCountT])
      itemState = getRuntimeContext.getListState(itemStateDesc)
    }
    override def processElement(i: ItemViewCountT, context: KeyedProcessFunction[Tuple, ItemViewCountT, String]#Context, collector: Collector[String]): Unit = {
      itemState.add(i)
      context.timerService().registerEventTimeTimer(i.windowEnd+1)
    }



    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCountT, String]#OnTimerContext, out: Collector[String]): Unit =
      {
        val  allItems:ListBuffer[ItemViewCountT]  = ListBuffer()
        import scala.collection.JavaConversions._
        for(item <-itemState.get()){
            allItems+=item
        }
        itemState.clear()
        val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
        // 将排名数据格式化，便于打印输出
        val result: StringBuilder = new StringBuilder
        result.append("====================================\n")
        result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

        for( i <- sortedItems.indices ){
          val currentItem: ItemViewCountT = sortedItems(i)
          // 输出打印的格式 e.g.  No1：  商品ID=12224  浏览量=2413
          result.append("No").append(i+1).append(":")
            .append("  商品ID=").append(currentItem.itemId)
            .append("  浏览量=").append(currentItem.count).append("\n")
        }
        result.append("====================================\n\n")
        // 控制输出频率
        Thread.sleep(1000)
        out.collect(result.toString)
      }
  }

}

