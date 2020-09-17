package com.swenchao.mr.selfInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @Author: Swenchao
 * @Date: 2020/9/8 下午 03:19
 * @Func:
 */
public class SelfInputRecordReder extends RecordReader<Text, BytesWritable> {

    FileSplit split;
    Configuration conf;
    Text k = new Text();
    BytesWritable v = new BytesWritable();
    boolean isProgress = true;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 初始化
        this.split = (FileSplit)inputSplit;

        conf = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // 业务逻辑处理(封装最终结果)

        if (isProgress){
            byte[] buf = new byte[(int)split.getLength()];
            FileSystem fs = null;
            FSDataInputStream fis = null;

            // 获取fs对象（每个切片都能拿到相应的路径）
            Path path= split.getPath();
            fs = path.getFileSystem(conf);

            // 获取输入流
            fis = fs.open(path);

            // 拷贝(将fis信息读取到buf中；0是从哪开始读；buf.length读取长度)
            IOUtils.readFully(fis, buf, 0, buf.length);

            // 写入v
            v.set(buf, 0, buf.length);

            // 封装k
            k.set(path.toString());

            // 关闭集群
            IOUtils.closeStream(fis);

            isProgress = false;

            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
       return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    /**
     * 获取正在处理进程
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
