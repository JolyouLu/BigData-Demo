package top.jolyoulu.diypartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: JolyouLu
 * @Date: 2022/9/18 19:56
 * @Version 1.0
 */
public class FlowCountReducer extends Reducer<Text, FlowBean,Text, FlowBean> {
    FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0;
        long sumDoFlow = 0;
        //1.累加求和
        for (FlowBean flowBean : values) {
            sumUpFlow += flowBean.getUpFlow();
            sumDoFlow += flowBean.getDownFlow();
        }
        v.set(sumUpFlow,sumDoFlow);
        //2.求和
        context.write(key,v);
    }
}
