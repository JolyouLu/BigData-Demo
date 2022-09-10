package top.jolyoulu.wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: JolyouLu
 * @Date: 2022/9/10 17:21
 * @Version 1.0
 */
//KEYIN map阶段的key
//VALUEIN map阶段的value
//KEYOUT
//VALUEOUT
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //从map获取出数据进行处理
        int sum = 0;
        //1.累加求和
        for (IntWritable value : values) {
            sum += value.get();
        }
        v.set(sum);
        //2.写出
        context.write(key,v);
    }
}
