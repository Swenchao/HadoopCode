package com.swenchao.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.jar.JarOutputStream;

/**
 * @Author: Swenchao
 * @Date: 2020/9/7 10:59
 * @Func: reducer程序
 */
public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);

        long sumUpFlow = 0;
        long sumDownFlow = 0;

//        System.out.println("==================================");
//        System.out.println(values);

        // 累加求和
        for (FlowBean flowBean : values){
//            System.out.println("---------------------------");
//            System.out.println(flowBean);
            sumUpFlow += flowBean.getUpFlow();
            sumDownFlow += flowBean.getDownFlow();
        }

        v.set(sumUpFlow, sumDownFlow);

        // 写出

        context.write(key, v);
    }
}
