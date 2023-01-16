package top.jolyoulu.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import top.jolyoulu.kvwordcount.KVTextDriver;
import top.jolyoulu.kvwordcount.KVTextMapper;
import top.jolyoulu.kvwordcount.KVTextReducer;
import top.jolyoulu.mapjion.DistributedCacheDriver;
import top.jolyoulu.mapjion.DistributedMapper;
import top.jolyoulu.sort.FlowCountDriver;

import java.io.File;
import java.net.URL;

/**
 * 多job串联案例
 * 先运行 OneIndexDriver 再运行 TwoIndexDriver
 * @Author: JolyouLu
 * @Date: 2023/1/8 16:57
 * @Version 1.0
 */
public class OneIndexDriver {
    public static void main(String[] args) throws Exception {
        //写死路径测试，读取于输出到在target目录下
        URL resource = FlowCountDriver.class.getClassLoader().getResource("test/index");
        File file = new File(resource.toURI());
        args = new String[]{
                file.getPath(),
                file.getPath()+"\\one"
        };

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(OneIndexDriver.class);

        job.setMapperClass(OneIndexMapper.class);
        job.setReducerClass(OneIndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
