package org.haoxin.bigdata.flink.watermark;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/9/3 18:04
 */
public class MyWatermark implements AssignerWithPeriodicWatermarks<Tuple3<Long, String, String>> {

    Long currentMaxTimeStamp =0L;
    final Long maxOutOfOrderness = 10000L;//最大允许的乱序时间是10s

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimeStamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Tuple3<Long, String, String> element, long previousElementTimestamp) {
        Long timestamp = element.f0;
        currentMaxTimeStamp = Math.max(timestamp,currentMaxTimeStamp);
        return timestamp;
    }
}
