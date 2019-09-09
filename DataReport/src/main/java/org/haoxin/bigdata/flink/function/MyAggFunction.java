package org.haoxin.bigdata.flink.function;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/9/3 18:21
 */
public class MyAggFunction implements WindowFunction<Tuple3<Long, String, String>, Tuple4<String,String,String,Long>, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<Long, String, String>> input, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
        //获取分组数据
        String type = tuple.getField(0).toString();
        String area = tuple.getField(1).toString();

        Iterator<Tuple3<Long, String, String>> it = input.iterator();
        ArrayList<Long> arrayList = new ArrayList<>();
        long count = 0;
        while (it.hasNext()){
            Tuple3<Long, String, String> next = it.next();
            arrayList.add(next.f0);
            count++;
        }
        Collections.sort(arrayList);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time = sdf.format(window.getEnd());

        //组装结果
        Tuple4<String, String, String, Long> res = new Tuple4<>(time, type, area, count);
        out.collect(res);

    }
}
