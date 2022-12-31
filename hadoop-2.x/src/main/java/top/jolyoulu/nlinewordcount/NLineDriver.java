package top.jolyoulu.nlinewordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @Author: JolyouLu
 * @Date: 2022/9/25 15:32
 * @Version 1.0
 */
public class NLineDriver {
    public static void main(String[] args) throws Exception {
        args = new String[]{
                "C:\\Users\\mi\\Downloads\\test\\inputnline.txt",
                "C:\\Users\\mi\\Downloads\\test\\inputnline"
        };
        Configuration conf = new Configuration();
        //获取job对象
        Job job = Job.getInstance(conf);
        //设置切片与切片规则
        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job,2);
        //设置jar存储位置
        job.setJarByClass(NLineDriver.class);
        //关联map与reduce类
        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);
        //设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置最终输出key value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
