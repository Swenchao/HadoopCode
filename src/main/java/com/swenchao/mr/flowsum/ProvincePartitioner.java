package com.swenchao.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author: Swenchao
 * @Date: 2020/9/9 下午 03:54
 * @Func: 自定义分区类
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        // k是手机号  v是流量信息

        // 获取手机号前三位（截取左闭右开）
        String prePhoneNum = text.toString().substring(0, 3);

        // 根据前三位进行分区
        int partition = 4;

        if ("136".equals(prePhoneNum)){
            partition = 0;
        }  else if ("137".equals(prePhoneNum)){
            partition = 1;
        } else if ("138".equals(prePhoneNum)){
            partition = 2;
        } else if ("139".equals(prePhoneNum)){
            partition = 3;
        }

        return partition;
    }
}
