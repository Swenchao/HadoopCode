package com.swenchao.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: Swenchao
 * @Date: 2020/9/9 下午 08:13
 * @Func: reducer类
 */
public class FlowCountSortReducer extends Reducer<FlowSortBean, Text, Text, FlowSortBean> {
    @Override
    protected void reduce(FlowSortBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values){
            context.write(value, key);
        }
    }
}
