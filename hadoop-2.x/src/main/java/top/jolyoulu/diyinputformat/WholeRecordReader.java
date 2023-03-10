package top.jolyoulu.diyinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Author: JolyouLu
 * @Date: 2022/9/25 16:02
 * @Version 1.0
 * 一个切片读取一次
 */
public class WholeRecordReader extends RecordReader<Text, BytesWritable> {

    FileSplit split;
    Configuration configuration;
    Text k = new Text();
    BytesWritable v = new BytesWritable();
    boolean isProgress = true;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //初始化
        this.split = (FileSplit) split;
        //获取配置信息
        configuration = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //核心处理逻辑
        if (isProgress){
            //获取fs对象(通过切片路径获取文件系统)
            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(configuration);
            //获取输入流
            FSDataInputStream fis = fs.open(path);
            //将输入流写入到一个缓冲区
            byte[] buf = new byte[(int) split.getLength()]; //定义一个中间缓存buf
            IOUtils.readFully(fis,buf,0,buf.length);
            //将缓冲区的字节流写入到value中
            v.set(buf,0,buf.length);
            //封装key
            k.set(path.toString());
            //关闭资源
            IOUtils.closeStream(fis);
            isProgress = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        //获取当前的Key
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        //获取当前value
        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
