package com.swenchao.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: Swenchao
 * @Date: 2020/9/10 上午 10:12
 * @Func: Combiner类
 */
public class WordcountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        // 累加求和
        for(IntWritable value : values){
            sum += value.get();
        }

        v.set(sum);

        // 写出
        context.write(key, v);

    }
}
