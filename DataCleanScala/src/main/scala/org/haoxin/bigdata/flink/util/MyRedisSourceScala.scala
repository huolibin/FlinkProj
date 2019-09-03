package org.haoxin.bigdata.flink.util



import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable

/**
  * @author huolibin@haoxin.cn
  * @date Created by sheting on 2019/9/3 11:48
  *
  */
class MyRedisSourceScala extends SourceFunction[mutable.Map[String,String]]{

  val logger = LoggerFactory.getLogger("MyRedisSourceScala")
  var jedis: Jedis = _
  var isRunning = true
  var keyValueMap = mutable.Map[String,String]()
  val SLEEP_MIN = 60000

  override def run(ctx: SourceFunction.SourceContext[mutable.Map[String, String]]): Unit = {
    //隐式转换，把java的Hashmap转化为scala的map
    import scala.collection.JavaConversions.mapAsScalaMap

    this.jedis = new Jedis("192.168.71.10",6379)
    while (isRunning){
      try {
        keyValueMap.clear()
        val map: mutable.Map[String, String] = jedis.hgetAll("areas")
        for (k <- map.keys.toList) {
          val value = map.get(k).get
          val splits = value.split(",")
          for (split <- splits) {
            keyValueMap += (split -> k)
          }
        }
        if (keyValueMap.nonEmpty) {
          ctx.collect(keyValueMap)
        } else {
          logger.warn("从redis中取出的数据为空！")
        }
        Thread.sleep(SLEEP_MIN)
      }catch {
        case e: JedisConnectionException =>{
          logger.error("redis链接异常，重新获取链接",e.getCause)
          jedis = new Jedis("192.168.71.10",6379)
        }
        case e: Exception => {
          logger.error("source数据源异常",e.getCause)
        }

      }
    }

  }

  override def cancel(): Unit = {
    isRunning = false
    if (jedis != null){
      jedis.close()
    }
  }
}
