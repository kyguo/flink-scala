package com.didichuxing.aggregation.functions

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

/**
  * 使用mapWithState方法完后自定义聚合和java的类似 https://github.com/kyguo/Flink/blob/master/src/main/java/com/didichuxing/aggregation/functions/KeyedGroupSumFuntion.java
  */

object KeyedGroupSumFuntion {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000);
    val lines: DataStream[String] = env.socketTextStream("localhost",8888)
    val keyed: KeyedStream[(String, Int), Tuple] = lines.flatMap(_.split(" ")).map { w =>
      if (w.equals("storm")) {
        print(1 / 0) //抛异常
      }
      (w, 1)
    }.keyBy(0)
    val res: DataStream[(String, Int)] = keyed.mapWithState((input: (String, Int), state: Option[Int]) =>
      state match {
        case Some(x) => ((input._1, input._2 + x), Some(x + input._2))
        case None => (input, Some(input._2))
      }

    )
    res.print();
    env.execute("KeyedGroupSumFuntions")
  }

}
