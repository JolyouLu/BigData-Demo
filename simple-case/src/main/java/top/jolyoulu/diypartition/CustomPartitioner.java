package top.jolyoulu.diypartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import top.jolyoulu.diypartition.FlowBean;

import java.security.Key;

/**
 * @Author: JolyouLu
 * @Date: 2022/10/7 16:30
 * @Version 1.0
 * Partitioner<Map的key, Map的value>
 * 154 155 167分开存，其余放到系统的分区中
 */
public class CustomPartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {
        //key 手机号 value 数据
        //获取手机号前三位
        String preNum = key.toString().substring(0, 3);
        switch (preNum){
            case "154":
                numPartitions = 0;
                break;
            case "155":
                numPartitions = 1;
                break;
            case "156":
                numPartitions = 2;
                break;
            default:
                numPartitions = 3;
                break;
        }
        return numPartitions;
    }
}
