package top.jolyoulu.order;

import com.sun.org.apache.bcel.internal.util.BCELifier;
import org.apache.avro.Schema;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringJoiner;

/**
 * @Author: JolyouLu
 * @Date: 2022/11/13 20:25
 * @Version 1.0
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private int orderId;    //订单id
    private double price;   //价格

    public OrderBean() {
    }

    public OrderBean(int orderId, double price) {
        this.orderId = orderId;
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean bean) {
        //先根据订单id排序，如果相同按照价格降序
        int res;
        if (orderId > bean.getOrderId()){
            res = 1;
        } else if (orderId < bean.getOrderId()) {
            res = -1;
        }else {
            if (price > bean.getPrice()){
                res = -1;
            }else if (price < bean.getPrice()) {
                res = 1;
            }else {
                res = 0;
            }
        }
        return res;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(orderId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        orderId = in.readInt();
        price = in.readDouble();
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }
}
