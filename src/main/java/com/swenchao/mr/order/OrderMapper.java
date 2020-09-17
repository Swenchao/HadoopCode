package com.swenchao.mr.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: Swenchao
 * @Date: 2020/9/10 下午 02:11
 * @Func: mapper类
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
//    0000001 Pdt_01 222.8

    OrderBean bean = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取一行
        String line = value.toString();

        // 切割取价格
        String[] details = line.split(" ");
        int orderId = Integer.parseInt(details[0]);
        double price = Double.parseDouble(details[2]);

        bean.setOrderId(orderId);
        bean.setPrice(price);

        context.write(bean, NullWritable.get());

    }
}
