package top.jolyoulu.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * @Author: JolyouLu
 * @Date: 2022/11/12 19:14
 * @Version 1.0
 */
public class FlowCountDriver {
    public static void main(String[] args) throws Exception {
        //写死路径测试，读取于输出到在target目录下
        URL resource = FlowCountDriver.class.getClassLoader().getResource("test/sort/test_data.txt");
        File file = new File(resource.toURI());
        args = new String[]{
                file.getPath(),
                file.getParent()+"\\out"
        };

        Configuration conf = new Configuration();
        //获取job对象
        Job job = Job.getInstance(conf);
        //设置jar存储位置
        job.setJarByClass(FlowCountDriver.class);
        //关联Map和Reduce类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);
        //设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        //设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //设置分区
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(4);
        //设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
