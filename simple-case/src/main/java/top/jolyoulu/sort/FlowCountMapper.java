package top.jolyoulu.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.IOException;

/**
 * @Author: JolyouLu
 * @Date: 2022/11/12 19:06
 * @Version 1.0
 */
public class FlowCountMapper extends Mapper<LongWritable, Text,FlowBean,Text> {

    FlowBean k = new FlowBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        String phoneNum = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        long sumFlow = Long.parseLong(fields[3]);

        k.setDownFlow(downFlow);
        k.setUpFlow(upFlow);
        k.setSumFlow(sumFlow);

        v.set(phoneNum);

        context.write(k,v);
    }
}
