package org.haoxin.flink.DataClean.source;


import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;

/**
 * 连接redis的source
 * reis中保存的是国家 大区的数据
 * hset areas AREA_US US
 * hset areas AREA_CT TW,HK
 * hset areas AREA_AR PK,KW,SA
 * hset areas AREA_IN IN
 * <p>
 * 需要把大区 国家的对应关系组装成hashmap
 *
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/8/23 10:36
 */
public class MyRedisSource implements SourceFunction<HashMap<String, String>> {
    private Logger logger = LoggerFactory.getLogger(MyRedisSource.class);
    private boolean isRunning = true;
    private Jedis jedis = null;
    private final long SLEEP_MINITES = 60000;

    @Override
    public void run(SourceContext<HashMap<String, String>> ctx) throws Exception {

        jedis = new Jedis("192.168.71.10", 6379);
        HashMap<String, String> hashMap = new HashMap<>();
        while (isRunning) {
            try {
                hashMap.clear();//每次运行前清除旧数据
                Map<String, String> areas = jedis.hgetAll("areas");
                for (Map.Entry<String, String> entry : areas.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    String[] splits = value.split(",");
                    for (String split : splits
                    ) {
                        hashMap.put(split, key);
                    }
                }
                if (hashMap.size() > 0) {
                    ctx.collect(hashMap);
                }else {
                    logger.warn("从redis中获取的数据为空！");
                }
                Thread.sleep(SLEEP_MINITES);
            }catch (JedisConnectionException e){
                logger.error("redis链接异常，重新链接",e.getCause());
                jedis = new Jedis("192.168.71.10", 6379);
            }catch (Exception e){
                logger.error("source异常",e.getCause());
            }

        }

    }

    @Override
    public void cancel() {
        isRunning = false;
        if (jedis != null) {
            jedis.close();
        }
    }
}
