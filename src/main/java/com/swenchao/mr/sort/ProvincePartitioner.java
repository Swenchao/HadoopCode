package com.swenchao.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author: Swenchao
 * @Date: 2020/9/9 下午 09:37
 * @Func:
 */
public class ProvincePartitioner extends Partitioner<FlowSortBean, Text> {

    @Override
    public int getPartition(FlowSortBean flowSortBean, Text text, int i) {
        // 按照手机号前三位分区

        // 获取前三位
        String prePhoneNum = text.toString().substring(0, 3);

        // 根据前三位进行分区
        int partition = 4;

        if (prePhoneNum.equals("136")){
            partition = 0;
        } else if (prePhoneNum.equals("137")){
            partition = 1;
        } if (prePhoneNum.equals("138")){
            partition = 2;
        } if (prePhoneNum.equals("139")){
            partition = 3;
        }

        return partition;
    }
}
