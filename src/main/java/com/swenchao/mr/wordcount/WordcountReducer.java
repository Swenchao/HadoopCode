package com.swenchao.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reduce阶段
 * @author swenchao
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable value = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        // 累加求和
        for (IntWritable value : values){
            sum += value.get();
        }

        value.set(sum);

        // 写出
        context.write(key, value);
    }
}
