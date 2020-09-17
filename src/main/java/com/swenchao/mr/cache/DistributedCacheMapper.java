package com.swenchao.mr.cache;

import com.swenchao.mr.joinTable.TableBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: Swenchao
 * @Date: 2020/9/15 上午 09:24
 * @Func: 缓存mapper
 */
public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Map<String, String> pdMap = new HashMap<>();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 缓存小表
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())){

            // 获取数据不为空，进行切割
            String[] detail = line.split("\t");
            pdMap.put(detail[0], detail[1]);
        }

        IOUtils.closeStream(reader);

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 拿到一行进行切割
        String line = value.toString();
        String[] detail = line.split("\t");

        // 从map里面获取pName
        String pid = detail[1];
        String pName = pdMap.get(pid);

        // 封装
        line = line + "\t" + pName;

        k.set(line);

        context.write(k, NullWritable.get());
    }
}
