package com.swenchao.mr.nline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: Swenchao
 * @Date: 2020/9/8 上午 09:48
 * @Func: Reducer方法
 */
public class NLineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable value = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0 ;
        for (IntWritable v : values){
            sum += v.get();
        }

        value.set(sum);

        context.write(key, value);

    }
}
