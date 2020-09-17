package com.swenchao.mr.selfInputFormat;

import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


/**
 * @Author: Swenchao
 * @Date: 2020/9/8 下午 03:13
 * @Func: 自定义Inputformat类
 */
public class SelfInput extends FileInputFormat<Text, BytesWritable> {

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        SelfInputRecordReder recordReder = new SelfInputRecordReder();
        recordReder.initialize(inputSplit, taskAttemptContext);
        return recordReder;
    }
}

