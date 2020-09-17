package com.swenchao.mr.kv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: Swenchao
 * @Date: 2020/9/7 20:26
 * @Func: reducer方法
 */
public class KVTextReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value : values){
            sum += value.get();
        }

        v.set(sum);
        context.write(key, v);
    }
}
