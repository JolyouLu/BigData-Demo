package top.jolyoulu.diyoutputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @Author: JolyouLu
 * @Date: 2022/11/19 16:17
 * @Version 1.0
 */
public class DiyRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream baidu;
    FSDataOutputStream other;

    public DiyRecordWriter(TaskAttemptContext job) {
        try {
            //获取文件系统
            FileSystem fs = FileSystem.get(job.getConfiguration());
            //创建输出的流
            baidu = fs.create(new Path("C:/Users/mi/Downloads/baidu.log"));
            other = fs.create(new Path("C:/Users/mi/Downloads/other.log"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        //判断key中是否有www.baidu.com 写出到baidu
        //非baidu.com 写出到other
        if (key.toString().contains("baidu")){
            baidu.write(key.toString().getBytes());
        }else {
            other.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(baidu);
        IOUtils.closeStream(other);
    }
}
