package top.jolyoulu.kvwordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Author: JolyouLu
 * @Date: 2022/9/25 14:04
 * @Version 1.0
 */
public class KVTextDriver {
    public static void main(String[] args) throws Exception {
        args = new String[]{
                "C:\\Users\\mi\\Downloads\\test\\inputkv.txt",
                "C:\\Users\\mi\\Downloads\\test\\inputkv"
        };
        Configuration conf = new Configuration();
        //设置用空格作为间隔符
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
        //获取job对象
        Job job = Job.getInstance(conf);
        //使用KV格式化
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        //设置jar存储位置
        job.setJarByClass(KVTextDriver.class);
        //关联map与reduce类
        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTextReducer.class);
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
