package top.jolyoulu.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;

/**
 * @Author: JolyouLu
 * @Date: 2023/1/8 17:07
 * @Version 1.0
 */
public class TwoIndexMapper extends Mapper<LongWritable, Text,Text,Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
//        a--1.txt	4
//        a--2.txt	2
//        a--3.txt	1
        //获取一行
        String lien = value.toString();
        //切割
        String[] fields = lien.split("--");
        //封装
        k.set(fields[0]);
        v.set(fields[1]);
        //写出
        context.write(k,v);
    }
}
