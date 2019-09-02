package org.haoxin.flink.DataClean;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;
import org.haoxin.flink.DataClean.source.MyRedisSource;

import java.util.HashMap;
import java.util.Properties;

/**
 * 数据清洗
 *
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/8/22 16:46
 */
public class DataClean {
    public static void main(String[] args) throws Exception {
        //flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        //checkpoint配置
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置statebackend
        //env.setStateBackend(new RocksDBStateBackend("hdfs://hdp1:9000/flink/checkpoints",true));


        //指定kafkasource
        String topic = "allData";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","192.168.71.10:9092");
        prop.setProperty("group.id","conn1");

        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>(topic, new SimpleStringSchema(), prop);

        //获取kafka中的数据
        //{"dt":"2019-08-12 23:33:11","countryCode":"US","data":[{"type":"s1","score":0.3,"level":"A"},{"type":"s1","score":0.3,"level":"A"},....]}
        DataStreamSource<String> data = env.addSource(myConsumer);

        //获取reids中国家码大区对应的数据,broadcast可以把数据发送到后面所有的算子中
        DataStream<HashMap<String, String>> mapData = env.addSource(new MyRedisSource()).broadcast();

        DataStream<String> resData = data.connect(mapData).flatMap(new CoFlatMapFunction<String, HashMap<String, String>, String>() {
            //存储国家和大区的映射
            private HashMap<String, String> allMap = new HashMap<String, String>();

            //flatmap1处理kafka中的数据
            @Override
            public void flatMap1(String value, Collector<String> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String dt = jsonObject.getString("dt");
                String countryCode = jsonObject.getString("countryCode");
                String area = allMap.get(countryCode);//获取大区
                JSONArray jsonArray = jsonObject.getJSONArray("data");
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jsonObject1 = jsonArray.getJSONObject(i);
                    jsonObject1.put("area", area);
                    jsonObject1.put("dt", dt);
                    out.collect(jsonObject1.toJSONString());
                }
            }

            //flatmap1处理redis中的数据
            @Override
            public void flatMap2(HashMap<String, String> value, Collector<String> out) throws Exception {
                this.allMap = value;
            }
        });

        /**
         * sink 是kafka
         */

        String outTopic = "allDataClean";
        Properties outprop = new Properties();
        outprop.put("bootstrap.servers","192.168.71.10:9092,192.168.71.11:9092,192.168.71.12:9092");
        outprop.put("transaction.timeout.ms",60000*15+"");//设置kafka里面的超时时间

        //kafka 0.11
        //FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(outTopic, new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()), outprop, FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);
        //kafka 0.10
        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<>(outTopic, new SimpleStringSchema(), outprop);
        resData.addSink(myProducer);

        env.execute("DataClean");
    }
}
