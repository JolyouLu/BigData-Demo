package top.jolyoulu.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import top.jolyoulu.sort.FlowCountDriver;

import java.io.File;
import java.net.URL;

/**
 * @Author: JolyouLu
 * @Date: 2023/1/8 17:17
 * @Version 1.0
 */
public class TwoIndexDriver {
    public static void main(String[] args) throws Exception {
        //写死路径测试，读取于输出到在target目录下
        URL resource = FlowCountDriver.class.getClassLoader().getResource("test/index/one/part-r-00000");
        File file = new File(resource.toURI());
        args = new String[]{
                file.getPath(),
                file.getParentFile().getParent()+"\\two"
        };

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(TwoIndexDriver.class);

        job.setMapperClass(TwoIndexMapper.class);
        job.setReducerClass(TwoIndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
