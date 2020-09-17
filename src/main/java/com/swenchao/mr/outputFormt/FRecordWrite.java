package com.swenchao.mr.outputFormt;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @Author: Swenchao
 * @Date: 2020/9/12 下午 05:21
 * @Func: 自定义 RecordWrite
 */
public class FRecordWrite extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream fosBaidu;
    FSDataOutputStream fosOther;

    public FRecordWrite(TaskAttemptContext taskAttemptContext) {

        try {
            // 获取文件系统
            FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());

            // 创建输出到baidu.log输出流
            fosBaidu = fs.create(new Path("baidu.log"));

            // 创建输出到other.log输出流
            fosOther = fs.create(new Path("other.log"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {

        // 判断key中是否有baidu，分别写入
        if (text.toString().contains("baidu")){
            fosBaidu.write(text.toString().getBytes());
        } else {
            fosOther.write(text.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        // 结束关掉资源
        IOUtils.closeStream(fosBaidu);
        IOUtils.closeStream(fosOther);
    }
}
