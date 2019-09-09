package org.haoxin.bigdata.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.OutputTag;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.haoxin.bigdata.flink.function.MyAggFunction;
import org.haoxin.bigdata.flink.watermark.MyWatermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/9/3 17:26
 */
public class DataReport {
    static Logger logger = LoggerFactory.getLogger(DataReport.class);

    public static void main(String[] args) {

        //flink的运行环境及线程数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        //设置使用eventtime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //checkpoint配置
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置statebackend
        //env.setStateBackend(new RocksDBStateBackend("hdfs://hdp1:9000/flink/checkpoints",true));

        //kafka source
        String intopic = "auditLog";
        Properties inprop = new Properties();
        inprop.setProperty("bootstrap.servers", "192.168.71.10:9092");
        inprop.setProperty("group.id", "consumer3");

        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>(intopic, new SimpleStringSchema(), inprop);
        //获取kafka数据,
        //数据格式: {"dt":"审核时间","type":"审核类型","username":"审核人姓名","area":"大区"}
        DataStreamSource<String> data = env.addSource(myConsumer);

        /**
         * 对数据清洗
         */

        DataStream<Tuple3<Long, String, String>> mapData = data.map(new MapFunction<String, Tuple3<Long, String, String>>() {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            @Override
            public Tuple3<Long, String, String> map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String dt = jsonObject.getString("dt");
                long time = 0;
                try {
                    time = sdf.parse(dt).getTime();
                } catch (ParseException e) {
                    logger.error("时间解析异常，dt:", e.getCause());
                }

                String type = jsonObject.getString("type");
                String area = jsonObject.getString("area");

                return new Tuple3<>(time, type, area);
            }
        });

        //过滤掉异常数据
        DataStream<Tuple3<Long, String, String>> filterData = mapData.filter(new FilterFunction<Tuple3<Long, String, String>>() {
            @Override
            public boolean filter(Tuple3<Long, String, String> value) throws Exception {
                boolean flag = true;
                if (value.f0 == 0) {
                    flag = false;
                }
                return flag;
            }
        });

        //保存迟到太久的数据
        OutputTag<Tuple3<Long, String, String>> tuple3OutputTag = new OutputTag<Tuple3<Long, String, String>>("late-data"){};

        /**
         * 窗口统计操作
         */

        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> resData = filterData.assignTimestampsAndWatermarks(new MyWatermark())
                .keyBy(1, 2)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .allowedLateness(Time.seconds(30))
                .sideOutputLateData(tuple3OutputTag)
                .apply(new MyAggFunction());

        //获取迟到的数据
        DataStream<Tuple3<Long, String, String>> sideOutput = resData.getSideOutput(tuple3OutputTag);

        //把迟到的数据存储到kafka中
        String outTopic = "lateLog";
        Properties outprop = new Properties();
        outprop.setProperty("bootstrap.servers", "192.168.71.10:9092");
        outprop.setProperty("transaction.timeout.ms",60000*15+"");
        FlinkKafkaProducer010<String> myProucer = new FlinkKafkaProducer010<>(outTopic, new SimpleStringSchema(), outprop);

        sideOutput.map(new MapFunction<Tuple3<Long, String, String>, String>() {
            @Override
            public String map(Tuple3<Long, String, String> value) throws Exception {
                return value.f0+"\t"+value.f1+"\t"+value.f2;
            }
        }).addSink(myProucer);

        resData.print();

        /**
         * 把计算结果存储到es中
         */
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("192.168.71.10",9200,"http"));

        ElasticsearchSink.Builder<Tuple4<String, String, String, Long>> esSinkBuilder = new ElasticsearchSink.Builder<Tuple4<String, String, String, Long>>(httpHosts, new ElasticsearchSinkFunction<Tuple4<String, String, String, Long>>() {

            public IndexRequest createIndexRequest(Tuple4<String, String, String, Long> element) {

                HashMap<String, Object> json = new HashMap<>();
                json.put("time", element.f0);
                json.put("type", element.f1);
                json.put("area", element.f2);
                json.put("count", element.f3);

                //使用time+type+area保证id唯一
                String id = element.f0.replace(" ", "_") + "-" + element.f1 + "-" + element.f2;

                return Requests.indexRequest()
                        .index("auditindex")
                        .type("audittype")
                        //.id(id)
                        .source(json);
            }

            @Override
            public void process(Tuple4<String, String, String, Long> stringStringStringLongTuple4, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                requestIndexer.add(createIndexRequest(stringStringStringLongTuple4));
            }
        });
        //设置批量写数据的缓冲区大小
        esSinkBuilder.setBulkFlushMaxActions(1);
        resData.addSink(esSinkBuilder.build());


        try {
            env.execute("DataReport");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
