package top.jolyoulu.nlinewordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: JolyouLu
 * @Date: 2022/9/25 15:27
 * @Version 1.0
 */
public class NLineMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
        //获取一行数据
        String line = value.toString();
        //切割
        String[] words = line.split(" ");
        //循环写出
        for (String word : words) {
            k.set(word);
            context.write(k,v);
        }
    }
}
