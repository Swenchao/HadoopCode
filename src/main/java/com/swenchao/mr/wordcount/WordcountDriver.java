package com.swenchao.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class WordcountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"D:/scwri/Desktop/input_words/", "D:/scwri/Desktop/output"};

        Configuration conf = new Configuration();

        // 开启map端输出压缩
        conf.setBoolean("mapreduce.map.output.compress", true);
        // 设置map端输出压缩方式
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

        // 获取job对象
        Job job = Job.getInstance(conf);


        // 设置reduce端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);

        // 设置压缩的方式
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        // 设置jar存储路径
//        job.setJar();  // 写死的，jar在什么地方，然后传的string就是什么地方
        // 动态获取（通过反射找到jar的主类）
        job.setJarByClass(WordcountDriver.class);

        // 关联map和reduce
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        // 关联Combiner
//        job.setCombinerClass(WordcountReducer.class);

        // 设置mapper阶段输出数据的k 和 v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置最终输出数据的k 和 v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 如果不设置InputFormat，它默认用的是TextInputFormat.class
//        job.setInputFormatClass(CombineTextInputFormat.class);

        //虚拟存储切片最大值设置4m
//        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        // 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0:1);
    }
}
