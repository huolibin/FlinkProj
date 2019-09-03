package org.haoxin.bigdata.flink

import java.util.Properties

import com.alibaba.fastjson
import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector
import org.haoxin.bigdata.flink.util.MyRedisSourceScala

import scala.collection.mutable
import scala.util.parsing.json.JSONObject

/**
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/9/3 10:48
  *
  */
object DataCleanScala {
  def main(args: Array[String]): Unit = {

    //获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //修改并行度
    env.setParallelism(3);
    //checkpoint配置
    env.enableCheckpointing(60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(3000)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //设置statebackend
    //env.setStateBackend(new RocksDBStateBackend("hdfs://hdp1:9000/flink/checkpoints",true));

    //隐式转换
    import  org.apache.flink.api.scala._

    //kafka sink
    val intopic = "allData"
    val inprop = new Properties()
    inprop.setProperty("bootstrap.servers", "192.168.71.10:9092")
    inprop.setProperty("group.id", "consumer2")

    val myConsumer = new FlinkKafkaConsumer010[String](intopic,new SimpleStringSchema(),inprop)
    //获取kafka数据
    val data: DataStream[String] = env.addSource(myConsumer)

    //从redis获取国家与大区的数据,把数据发送到后面的所有算子
    val mapData: DataStream[mutable.Map[String, String]] = env.addSource(new MyRedisSourceScala).broadcast

    //计算
    data.connect(mapData).flatMap(new CoFlatMapFunction[String,mutable.Map[String,String],String] {
      //存储国家和大区
      private var allMap: mutable.Map[String, String] = mutable.Map[String,String]()

      //kafka数据
      override def flatMap1(value: String, out: Collector[String]): Unit = {
        val jsonObject: fastjson.JSONObject = JSON.parseObject(value)
        val dt = jsonObject.getString("dt")
        val countryCode = jsonObject.getString("countryCode")
        val area = allMap.get(countryCode).getOrElse("other")
        val arrayJson: JSONArray = jsonObject.getJSONArray("data")
        for(i <- 0 to arrayJson.size()-1){
          val jsonObject1: fastjson.JSONObject = arrayJson.getJSONObject(i)
          jsonObject1.put("dt",dt)
          jsonObject1.put("area",area)
          out.collect(jsonObject1.toJSONString)
        }

      }

      //redis数据
      override def flatMap2(value: mutable.Map[String, String], out: Collector[String]): Unit = {
        this.allMap = value
      }
    })
  }

}
