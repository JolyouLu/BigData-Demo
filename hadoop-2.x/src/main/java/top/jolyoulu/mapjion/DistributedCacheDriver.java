package top.jolyoulu.mapjion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import top.jolyoulu.order.*;
import top.jolyoulu.reducejion.TableBean;
import top.jolyoulu.reducejion.TableDriver;
import top.jolyoulu.reducejion.TableMapper;
import top.jolyoulu.reducejion.TableReducer;
import top.jolyoulu.sort.FlowCountDriver;

import java.io.File;
import java.net.URI;
import java.net.URL;

/**
 * @Author: JolyouLu
 * @Date: 2022/11/20 18:07
 * @Version 1.0
 */
public class DistributedCacheDriver {
    public static void main(String[] args) throws Exception {
        //写死路径测试，读取于输出到在target目录下
        URL resource = FlowCountDriver.class.getClassLoader().getResource("test/mapjion/order.txt");
        File file = new File(resource.toURI());
        args = new String[]{
                file.getPath(),
                file.getParent()+"\\out"
        };

        Configuration conf = new Configuration();
        //获取job对象
        Job job = Job.getInstance(conf);
        //设置jar存储位置
        job.setJarByClass(DistributedCacheDriver.class);
        //关联Map，Mapjion无需Reduce
        job.setMapperClass(DistributedMapper.class);
        //设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //加载缓存数据
        job.addCacheFile(FlowCountDriver.class.getClassLoader().getResource("test/mapjion/pd.txt").toURI());
        //Mapjion无需Reduce，设置reduceTask数量0
        job.setNumReduceTasks(0);
        //提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
