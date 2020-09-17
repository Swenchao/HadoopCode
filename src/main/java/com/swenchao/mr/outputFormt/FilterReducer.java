package com.swenchao.mr.outputFormt;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: Swenchao
 * @Date: 2020/9/12 下午 04:59
 * @Func: Reducer类
 */
public class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    Text k = new Text();

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        String line = key.toString();
        line += "\r\n";

        k.set(line);

        context.write(k, NullWritable.get());
    }
}
