package com.swenchao.mr.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: Swenchao
 * @Date: 2020/9/9 下午 07:42
 * @Func: 自己定义的FlowBean对象
 */
public class FlowSortBean implements WritableComparable<FlowSortBean> {

    /**上行流量*/
    private long upFlow;
    /**下行流量*/
    private long downFlow;
    /**总流量*/
    private long sumFlow;

    public FlowSortBean() {
    }

    public FlowSortBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public void set(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    /**
     * 排序比较
     * @param bean
     * @return res
     */
    @Override
    public int compareTo(FlowSortBean bean) {

        int res;

        // 比较逻辑
        if (sumFlow < bean.getSumFlow()){
            res = 1;
        } else if (sumFlow > bean.getSumFlow()){
            res = -1;
        } else {
            res = 0;
        }

        return res;
    }

    /**
     * 序列化
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    /**
     * 反序列化
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
}
