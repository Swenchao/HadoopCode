package com.swenchao.mr.log;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: Swenchao
 * @Date: 2020/9/16 下午 04:23
 * @Func: Mapper类
 */
public class logMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取一行
        String line = value.toString();

        // 解析数据
        boolean result = parseLog(line, context);

        if (!result) {
            return;
        } else {
            context.write(value, NullWritable.get());
        }
    }

    private boolean parseLog(String line, Context context) {
        String[] detail = line.split(" ");

        if (detail.length > 11){
            context.getCounter("map", "true").increment(1);
            return true;
        } else {
            context.getCounter("map", "false").increment(1);
            return false;
        }
    }
}
