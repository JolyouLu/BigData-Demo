package top.jolyoulu.flowsum;

import org.apache.hadoop.io.Writable;

import javax.swing.plaf.PanelUI;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringJoiner;

/**
 * @Author: JolyouLu
 * @Date: 2022/9/18 19:33
 * @Version 1.0
 */
//1.实现Writable接口
public class FlowBean implements Writable {

    private long upFlow;   //上行流量
    private long downFlow; //下行流量
    private long sumFlow;  //总流量

    //2.生成空参构造
    public FlowBean() {
    }

    //3.重写序列化方法
    @Override
    public void write(DataOutput out) throws IOException {
        //注意：序列化与放序列化的顺序需要完全一致
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    //4.重写反序列化方法
    @Override
    public void readFields(DataInput in) throws IOException {
        //注意：序列化与放序列化的顺序需要完全一致
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }

    //5.重写toString()
    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void set(long sumUpFlow, long sumDoFlow) {
        this.upFlow = sumUpFlow;
        this.downFlow = sumDoFlow;
        this.sumFlow = sumUpFlow + sumDoFlow;
    }
}
