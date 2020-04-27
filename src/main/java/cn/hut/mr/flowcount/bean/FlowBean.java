package cn.hut.mr.flowcount.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description 流量实体类--自定义的一个实体类，如果想要在Hadoop的Mapper和Reducer中进行传输，我们需要
 *                         实现Hadoop序列化的接口【Writable、WritableComparable】
 *
 * @author HZX
 * @date 2020/4/23 10:23
 */
public class FlowBean implements Writable{

    // 封装属性
    private String telno = "";
    private long upFlow = 0; // 上行流量
    private long downFlow = 0; // 下行流量
    private long sumFlow = 0; // 总流量

    public FlowBean() {

    }

    private FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }
    public String getTelno(){
        return this.telno;
    }
    public FlowBean setTelno(String telno){
        this.telno = telno;
        return this;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public FlowBean setUpFlow(long upFlow) {
        this.upFlow = upFlow;
        this.sumFlow = this.downFlow + this.upFlow;
        return this;
    }

    public FlowBean setDownFlow(long downFlow) {
        this.downFlow = downFlow;
        this.sumFlow = this.downFlow + this.upFlow;
        return this;
    }

    private FlowBean setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
        return this;
    }

    private void set(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public FlowBean add(FlowBean o){

        this.setDownFlow(
            this.getDownFlow()+ o.getDownFlow()
        ).setUpFlow(
            this.getUpFlow()+o.getUpFlow()
        );
        return this;
    }

    @Override
    public String toString() {
        return "\t" + upFlow +
                "\t" + downFlow +
                "\t" + sumFlow;
    }

    /**
     * @Description 序列化：将要输入的数据转为字节流
     *
     * @param out
     * @return void
     */
    public void write(DataOutput out) throws IOException {
        out.writeUTF(telno);
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    /**
     * @Description 反序列化：从字节流中恢复出所需的字段【和序列化时候的顺序保持一致】
     *
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        this.telno = in.readUTF();
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    // public int compareTo(FlowBean o) {
    //     return (int)(o.getSumFlow() - this.getSumFlow());
    // }
}
