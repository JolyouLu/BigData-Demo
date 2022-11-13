package top.jolyoulu.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: JolyouLu
 * @Date: 2022/11/12 19:11
 * @Version 1.0
 */
public class FlowCountSortReducer extends Reducer<FlowBean, Text,Text,FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value,key);
        }
    }
}
