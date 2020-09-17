package com.swenchao.mr.flowsum;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: Swenchao
 * @Date: 2020/9/7 8:52
 * @Func: mapper程序
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);

        // 获取一行
        String line = value.toString();

        // 切割
        String[] fields = line.split("\t");
//        System.out.println("================================");
//        System.out.println(line);
        // 封装对象
        k.set(fields[1]);
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);
        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);

        // 写出
        context.write(k, v);

    }
}
