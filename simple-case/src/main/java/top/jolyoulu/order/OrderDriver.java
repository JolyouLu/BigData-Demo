package top.jolyoulu.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import top.jolyoulu.sort.*;

import java.io.File;
import java.net.URL;

/**
 * @Author: JolyouLu
 * @Date: 2022/11/13 20:39
 * @Version 1.0
 */
public class OrderDriver {
    public static void main(String[] args) throws  Exception{
        //写死路径测试，读取于输出到在target目录下
        URL resource = FlowCountDriver.class.getClassLoader().getResource("test/order/test_data.txt");
        File file = new File(resource.toURI());
        args = new String[]{
                file.getPath(),
                file.getParent()+"\\out"
        };

        Configuration conf = new Configuration();
        //获取job对象
        Job job = Job.getInstance(conf);
        //设置jar存储位置
        job.setJarByClass(OrderDriver.class);
        //关联Map和Reduce类
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);
        //设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置最终数据输出的key和value类型
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        //设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //设置reduce端的分组
        job.setGroupingComparatorClass(OrderGroupingComparator.class);
        //提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
