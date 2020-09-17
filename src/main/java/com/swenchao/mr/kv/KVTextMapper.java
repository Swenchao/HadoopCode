package com.swenchao.mr.kv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: Swenchao
 * @Date: 2020/9/7 20:26
 * @Func: mapper方法
 */
public class KVTextMapper extends Mapper<Text, Text, Text, IntWritable> {

    IntWritable v = new IntWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, v);
    }
}
