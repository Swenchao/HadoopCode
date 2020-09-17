package com.swenchao.mr.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: Swenchao
 * @Date: 2020/9/9 下午 08:13
 * @Func:
 */
public class FlowCountSortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"D:/scwri/Desktop/input_phone/phone_sum.txt", "D:/scwri/Desktop/output1"};

        Configuration conf = new Configuration();

        // 获取job对象
        Job job = Job.getInstance(conf);

        // 设置jar路径
        job.setJarByClass(FlowCountSortDriver.class);

        // 关联mapper和reducer
        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);

        // 设置mapper输出的kv类型
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(FlowSortBean.class);

        // 设置最终输出的kv类型
        job.setOutputKeyClass(FlowSortBean.class);
        job.setOutputValueClass(Text.class);

        // 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置分区和分区数
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);

        // 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0:1);
    }
}
