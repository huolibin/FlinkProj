package org.haoxin.bigdata.datareport

import java.text.{ParseException, SimpleDateFormat}
import java.util
import java.util.{Date, Properties}

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

/**
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/9/9 16:43
  *
  */
object DataReportScala {
  val logger = LoggerFactory.getLogger(DataReportScala.getClass)
  def main(args: Array[String]): Unit = {

    //flink的运行环境及线程数
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)

    //设置使用eventtime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //checkpoint配置
    env.enableCheckpointing(60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(3000)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //设置statebackend
    //env.setStateBackend(new RocksDBStateBackend("hdfs://hdp1:9000/flink/checkpoints",true));

    //kafka source
    val inTopic = "auditLog"
    val inprop = new Properties()
    inprop.setProperty("bootstrap.servers", "192.168.71.10:9092")
    inprop.setProperty("group.id", "consumer4")

    val myConsumer = new FlinkKafkaConsumer010[String](inTopic,new SimpleStringSchema(),inprop)

    import org.apache.flink.api.scala._
    //获取kafka数据
    val data = env.addSource(myConsumer)

    //对数据进行清洗
    //数据格式: {"dt":"审核时间","type":"审核类型","username":"审核人姓名","area":"大区"}
    val mapData = data.map(line => {
      val jsonObject = JSON.parseObject(line)
      val dt = jsonObject.getString("dt")
      var time = 0L
      try {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val date = sdf.parse(dt)
        time = date.getTime
      } catch {
        case e: ParseException => {
          logger.error("时间解析异常:" + dt, e.getCause)
        }
      }
      val type1 = jsonObject.getString("type")
      val area = jsonObject.getString("area")

      (time, type1, area)

    })
    //过滤异常数据
    val filterData = mapData.filter(_._1>0)

    //保存迟到的数据
    val outputTag = new OutputTag[Tuple3[Long,String,String]]("late_data"){}

    //window窗口函数计算
    val resData = filterData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, String)] {
      var currentMaxTimestamp = 0L
      var maxOutOfOrderness = 10000L //最大允许的乱序时间
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(element: (Long, String, String), previousElementTimestamp: Long): Long = {
        val timestamp = element._1
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
    }).keyBy(1, 2)
      .timeWindow(Time.minutes(1))
      .allowedLateness(Time.seconds(30))
      .sideOutputLateData(outputTag)
      .apply(new WindowFunction[Tuple3[Long, String, String], Tuple4[String, String, String, Long], Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, String)], out: Collector[(String, String, String, Long)]): Unit = {
          val type1 = key.getField(0)
          val area = key.getField(1)
          val it = input.iterator
          var count = 0
          val arrBuf = new ArrayBuffer[Long]()
          while (it.hasNext) {
            val next = it.next()
            count += 1
            arrBuf.append(next._1)
          }
          //排序
          Sorting.quickSort(arrBuf.toArray)

          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val time = sdf.format(new Date(arrBuf.last))

          //组装结果
          val res = new Tuple4[String, String, String, Long](time, type1, area, count)
          out.collect(res)
        }
      })
    //获取迟到数据
    val sideOutput: DataStream[(Long, String, String)] = resData.getSideOutput(outputTag)
    //将迟到数据存入kafka
    val outTopic = "lateLog"
    val outprop = new Properties()
    outprop.setProperty("bootstrap.servers", "192.168.71.10:9092")
    outprop.setProperty("transaction.timeout.ms",60000*15+"")

    val myProducer = new FlinkKafkaProducer010[String](outTopic,new SimpleStringSchema(),outprop)
    sideOutput.map(line => line._1+"\t"+line._2+"\t"+line._3).addSink(myProducer)

    //将结算结果存入es
    val httpHosts = new util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("192.168.71.10",9200,"http"))

    val esSinkBuilder: ElasticsearchSink.Builder[(String, String, String, Long)] = new ElasticsearchSink.Builder[Tuple4[String, String, String, Long]](httpHosts,
      new ElasticsearchSinkFunction[Tuple4[String, String, String, Long]] {
        def createIndexRequest(element: Tuple4[String, String, String, Long]): IndexRequest = {
          val json = new java.util.HashMap[String, Any]
          json.put("time", element._1)
          json.put("type", element._2)
          json.put("area", element._3)
          json.put("count", element._4)

          //唯一id
          val id = element._1.replace(" ", "_") + "-" + element._2 + "-" + element._3

          return Requests.indexRequest()
            .index("auditindex")
            .`type`("audittype")
            .id(id)
            .source(json)
        }

        override def process(t: (String, String, String, Long), runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(createIndexRequest(t))
        }
      })

    //设置缓存行数，再写入es
    esSinkBuilder.setBulkFlushMaxActions(10)
    resData.addSink(esSinkBuilder.build())

    env.execute("DataReportScala")

  }

}
