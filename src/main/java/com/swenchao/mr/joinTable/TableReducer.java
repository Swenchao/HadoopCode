package com.swenchao.mr.joinTable;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @Author: Swenchao
 * @Date: 2020/9/14 下午 03:55
 * @Func: reducer类
 */
public class TableReducer extends Reducer<Text, TableBean, NullWritable, TableBean> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

        ArrayList<TableBean> orderBeans = new ArrayList<>();

        /**标记是否获得pname*/
        boolean isGetPname = false;
        String pName = "";

        for (TableBean bean :values) {

            if ("order".equals(bean.getFlag())){

                TableBean tableBean = new TableBean();

                try {
                    BeanUtils.copyProperties(tableBean, bean);
                    orderBeans.add(tableBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }

            } else if (!isGetPname) {
                pName += bean.getPname();
                isGetPname = true;
            }
        }

        for (TableBean tableBean : orderBeans){
            tableBean.setPname(pName);
            context.write(NullWritable.get(), tableBean);
        }
    }
}
