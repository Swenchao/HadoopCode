package com.swenchao.mr.joinTable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import javax.swing.text.TabExpander;
import java.io.IOException;

/**
 * @Author: Swenchao
 * @Date: 2020/9/14 下午 03:55
 * @Func: mapper类
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    String name;
    TableBean bean = new TableBean();
    Text k = new Text();

    /**因为在一开始要拿到它的文件信息来做区分，所以重写此方法*/
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        // 获取文件名称
        FileSplit inputSplit = (FileSplit) context.getInputSplit();

        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        if (name.startsWith("order")) {

            String[] detail = line.split("\t");

            bean.setId(detail[0]);
            bean.setPid(detail[1]);
            bean.setAmount(Integer.parseInt(detail[2]));
            bean.setFlag("order");
            bean.setPname("");

            k.set(detail[1]);
        } else {

            String[] detail = line.split("\t");

            bean.setId("");
            bean.setPid(detail[0]);
            bean.setAmount(0);
            bean.setFlag("pd");
            bean.setPname(detail[1]);

            k.set(detail[0]);
        }

        context.write(k, bean);
    }

}
