package cn.hut.hdfs_demo.mr.flowsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {

    private String telno;
    private long upload;
    private long download;
    private long sumload;

    public FlowBean() {
    }

    public FlowBean setTelno(String telno) {
        this.telno = telno;
        return this;
    }

    public FlowBean setUpload(long upload) {
        this.upload = upload;
        return this;
    }

    public FlowBean setDownload(long download) {
        this.download = download;
        return this;
    }

    public FlowBean setSumload(long sumload) {
        this.sumload = sumload;
        return this;
    }

    public String getTelno() {
        return telno;
    }

    public long getUpload() {
        return upload;
    }

    public long getDownload() {
        return download;
    }

    public long getSumload() {
        return sumload;
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "telno='" + telno + '\'' +
                ", upload=" + upload +
                ", download=" + download +
                ", sumload=" + sumload +
                '}';
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(telno);
        out.writeLong(upload);
        out.writeLong(download);
        out.writeLong(sumload);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        telno = in.readUTF();
        upload = in.readLong();
        download = in.readLong();
        sumload = in.readLong();
    }

    @Override
    public int compareTo(FlowBean o) {
        return Long.compare(o.sumload, this.sumload);
    }
}
