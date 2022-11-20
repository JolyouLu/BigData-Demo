package top.jolyoulu.reducejion;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ID;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.map.util.BeanUtil;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: JolyouLu
 * @Date: 2022/11/20 14:10
 * @Version 1.0
 */
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        //存放所有订单集合
        List<TableBean> orders = new ArrayList<>();
        //存放所有产品集合
        TableBean pd = new TableBean();
        try {
            //将订单与产品分开存放
            for (TableBean tableBean : values) {
                if ("order".equals(tableBean.getFlag())) {
                    TableBean tmp = new TableBean();
                    BeanUtils.copyProperties(tmp,tableBean);
                    orders.add(tmp);
                }else {
                    BeanUtils.copyProperties(pd,tableBean);
                }
            }
            //将所有订单都设置好产品名称
            for (TableBean order : orders) {
                order.setPname(pd.getPname());
                context.write(order,NullWritable.get());
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
