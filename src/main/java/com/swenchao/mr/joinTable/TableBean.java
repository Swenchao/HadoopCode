package com.swenchao.mr.joinTable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: Swenchao
 * @Date: 2020/9/14 下午 03:03
 * @Func: 表的bean类
 */
public class TableBean implements Writable {

    /**订单id*/
    private String id;
    /**产品id*/
    private String pid;
    /**数量*/
    private int amount;
    /**产品名称*/
    private String pname;
    /**标记（订单还是产品）*/
    private String flag;

    public TableBean() {
    }

    public TableBean(String id, String pid, int amount, String pname, String flag) {
        this.id = id;
        this.pid = pid;
        this.amount = amount;
        this.pname = pname;
        this.flag = flag;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(id);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
        dataOutput.writeUTF(flag);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        id = dataInput.readUTF();
        pid = dataInput.readUTF();
        amount = dataInput.readInt();
        pname = dataInput.readUTF();
        flag = dataInput.readUTF();

    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return id + "\t" + amount + "\t" + pname;
    }
}
