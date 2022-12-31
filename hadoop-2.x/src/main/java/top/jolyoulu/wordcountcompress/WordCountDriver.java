package top.jolyoulu.wordcountcompress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import top.jolyoulu.sort.FlowCountDriver;

import java.io.File;
import java.net.URL;

/**
 * 压缩案例
 * @Author: JolyouLu
 * @Date: 2022/9/10 17:32
 * @Version 1.0
 */
public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        //写死路径测试，读取于输出到在target目录下
        URL resource = FlowCountDriver.class.getClassLoader().getResource("test/wordcountcompress/test_data.txt");
        File file = new File(resource.toURI());
        args = new String[]{
                file.getPath(),
                file.getParent() + "\\out"
        };

        Configuration conf = new Configuration();
        //打开map端输出压缩
        conf.setBoolean("mapreduce.map.output.compress", true);
        //设置map压缩方式
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
        //1.获取job对象
        Job job = Job.getInstance(conf);
        //2.设置jar存储位置
        job.setJarByClass(WordCountDriver.class);
        //3.关联Map和Reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //4.设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置reduce端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);
        //设置压缩方式
        FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.BZip2Codec.class);
        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
