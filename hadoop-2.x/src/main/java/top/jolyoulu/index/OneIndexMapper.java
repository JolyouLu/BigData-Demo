package top.jolyoulu.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Author: JolyouLu
 * @Date: 2023/1/8 16:49
 * @Version 1.0
 */
public class OneIndexMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    String name;
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //获取文件名称
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //切割
        String[] fields = line.split(" ");
        //写出
        for (String word : fields) {
            k.set(word+"--"+name);
            context.write(k,v);
        }
    }
}
