package com.swenchao.mr.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

/**
 * @Author: Swenchao
 * @Date: 2020/9/17 上午 10:54
 * @Func: 压缩
 */
public class TestCompress {
    public static void main(String[] args) throws IOException, ClassNotFoundException {

        // 压缩（可以更换不同压缩方式）
        compress("D:/scwri/Desktop/inputWeb/web.log","org.apache.hadoop.io.compress.BZip2Codec");

        // 解压缩
        decompress("d:/scwri/Desktop/inputWeb/web.log.bz2");
    }

    private static void decompress(String fileName) throws IOException {

        // 压缩方式检查
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(fileName));

        if (codec == null) {
            System.out.println("can not process");
            return;
        }

        // 获取输入流
        FileInputStream fis = new FileInputStream(new File(fileName));
        CompressionInputStream cis = codec.createInputStream(fis);

        // 获取输出流
        FileOutputStream fos = new FileOutputStream(new File(fileName + ".decode"));

        // 流的对拷
        IOUtils.copyBytes(cis, fos, 1024*1024, false);

        // 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fis);

    }

    private static void compress(String fileName, String method) throws IOException, ClassNotFoundException {
        //获取输入流
        FileInputStream fis = new FileInputStream(new File(fileName));

        Class<?> theClass = Class.forName(method);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(theClass, new Configuration());

        // 获取输出流(需要一个压缩后的扩展名)
        FileOutputStream fos = new FileOutputStream(new File(fileName + codec.getDefaultExtension()));
        CompressionOutputStream cos = codec.createOutputStream(fos);

        //流的对拷(参数：输入流、输出流、缓冲区（自己设置）、最后是否关闭输入流和输出流)
        IOUtils.copyBytes(fis, cos, 1024*1024, false);

        // 关闭资源
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);

    }
}
