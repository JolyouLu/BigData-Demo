package top.jolyoulu.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import javax.swing.text.TabExpander;

/**
 * @Author: JolyouLu
 * @Date: 2022/11/13 12:49
 * @Version 1.0
 */
public class ProvincePartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean key, Text value, int numPartitions) {
        //根据手机号码进行分区
        String preNum = value.toString().substring(0, 3);
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
