package top.jolyoulu.diyinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * @Author: JolyouLu
 * @Date: 2022/9/25 16:31
 * @Version 1.0
 */
public class SequenceDriver {
    public static void main(String[] args) throws Exception {
        args = new String[]{
                "C:\\Users\\mi\\Downloads\\test\\diyfromat",
                "C:\\Users\\mi\\Downloads\\test\\diyfromat_out"
        };
        //获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar，关联自定义mapper和reducer
        job.setJarByClass(SequenceDriver.class);
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);
        //设置inputFormat和outputFormat
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //设置map输出端的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        //设置最终输出端kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        //设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
