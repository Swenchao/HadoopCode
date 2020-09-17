package com.swenchao.mr.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: Swenchao
 * @Date: 2020/9/10 下午 01:47
 * @Func:
 */
public class OrderBean implements WritableComparable<OrderBean> {
    /**订单id*/
    private int orderId;
    /**价格*/
    private double price;

    public OrderBean() {
    }

    public OrderBean(int orderId, int price) {
        this.orderId = orderId;
        this.price = price;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean bean) {

        // 先按id升序排序，如果相同再降序排序
        int result = 1;

        if (orderId > bean.getOrderId()) {
            result = 1;
        } else if (orderId < bean.getOrderId()){
            result = -1;
        } else {
            if (price > bean.getPrice()){
                result = -1;
            } else if (price < bean.getPrice()){
                result = 1;
            } else {
                result = 0;
            }
        }

        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(orderId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readInt();
        price = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }
}
