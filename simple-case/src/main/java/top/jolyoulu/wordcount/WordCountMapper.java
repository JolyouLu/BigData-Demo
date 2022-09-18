package top.jolyoulu.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: JolyouLu
 * @Date: 2022/9/10 17:09
 * @Version 1.0
 */
//map阶段
//KEYIN 输入数据的key，内容是文本所以key是偏移量
//VALUEIN 数据数据类型value
//KEYOUT 输出数据类型
//VALUEOUT 数据数据value类型
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();
        //2.根据空格拆分
        String[] words = line.split(" ");
        //3.循环写出
        for (String word : words) {
            k.set(word);
            context.write(k,v);
        }
    }

}
