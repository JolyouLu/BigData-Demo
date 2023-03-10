package top.jolyoulu.diypartition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: JolyouLu
 * @Date: 2022/9/10 17:32
 * @Version 1.0
 */
public class FlowCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        args = new String[]{
                "C:\\Users\\mi\\Downloads\\test\\phone_data2.txt",
                "C:\\Users\\mi\\Downloads\\test\\phone_data2_out"
        };
        Configuration conf = new Configuration();
        //1.获取job对象
        Job job = Job.getInstance(conf);
        //2.设置jar存储位置
        job.setJarByClass(FlowCountDriver.class);
        //3.关联Map和Reduce类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);
        //4.设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //5.设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //设置自定义分区规则，NumReduceTasks要与自定义分区数量一直
        job.setPartitionerClass(CustomPartitioner.class);
        job.setNumReduceTasks(4);
        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
