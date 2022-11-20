package top.jolyoulu.reducejion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import top.jolyoulu.order.*;
import top.jolyoulu.sort.FlowCountDriver;

import java.io.File;
import java.net.URL;

/**
 * @Author: JolyouLu
 * @Date: 2022/11/20 14:44
 * @Version 1.0
 */
public class TableDriver {
    public static void main(String[] args) throws Exception {
        //写死路径测试，读取于输出到在target目录下
        URL resource = FlowCountDriver.class.getClassLoader().getResource("test/reducejion");
        File file = new File(resource.toURI());
        args = new String[]{
                file.getPath(),
                file.getPath()+"\\out"
        };

        Configuration conf = new Configuration();
        //获取job对象
        Job job = Job.getInstance(conf);
        //设置jar存储位置
        job.setJarByClass(TableDriver.class);
        //关联Map和Reduce类
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);
        //设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);
        //设置最终数据输出的key和value类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);
        //设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
