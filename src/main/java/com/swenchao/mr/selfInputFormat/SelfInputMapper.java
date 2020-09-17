package com.swenchao.mr.selfInputFormat;

import com.sun.crypto.provider.HmacPKCS12PBESHA1;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: Swenchao
 * @Date: 2020/9/8 下午 08:20
 * @Func:
 */
public class SelfInputMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
