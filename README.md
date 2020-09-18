# HadoopCode

学习Hadoop过程中所写demo实践

博客上已更新完成Hadoop学习笔记——https://swenchao.github.io/  建议配合使用

后期会持续更新Spark以及Hive学习笔记以及代码

## 目录结构

com.swenchao.mr.wordcount 最经典wordcount样例（其中有多处修改，这是最终数据压缩部分代码。里面有之前的都注释掉了，可以慢慢看）
com.swenchao.mr.selfInputFormat 自定义Inputformat尝试
com.swenchao.mr.log 数据清洗案例（网络日志筛选）
com.swenchao.mr.flowsum Partition分区案例+汇总（电话流量统计+分区）
com.swenchao.mr.compress    文件压缩
com.swenchao.mr.joinTable   Reduce Join（表连接）
com.swenchao.mr.cache   Map Join案例（小表连接（上面的衍生））   
com.swenchao.mr.nline  NLineInputFormat使用 
com.swenchao.mr.kv  KeyValueTextInputFormat使用
com.swenchao.mr.order   GroupingComparator分组（订单分组）
com.swenchao.mr.outputFormat    自定义outputFormat
com.swenchao.mr.sort    WritableComparable排序案例（对手机总流量进行排序）

**注意其中有些路径需要修改，其中每个文件样式博客中已经给出**