package com.swenchao.mr.order;

import com.swenchao.mr.sort.OrderSortGroupingComparator;
import com.swenchao.mr.wordcount.WordcountDriver;
import com.swenchao.mr.wordcount.WordcountMapper;
import com.swenchao.mr.wordcount.WordcountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: Swenchao
 * @Date: 2020/9/10 下午 02:11
 * @Func:
 */
public class OrderDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:/scwri/Desktop/input_order/", "D:/scwri/Desktop/output"};

        Configuration conf = new Configuration();
        // 获取job对象
        Job job = Job.getInstance(conf);

        // 设置jar存储路径
        job.setJarByClass(OrderDriver.class);

        // 关联map和reduce
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        // 设置mapper阶段输出数据的k 和 v类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置reduce分组
        job.setGroupingComparatorClass(OrderSortGroupingComparator.class);

        // 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0:1);
    }
}
