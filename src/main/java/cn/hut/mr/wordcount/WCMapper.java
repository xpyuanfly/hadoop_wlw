package cn.hut.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/// k1 v1 k2 v2必须是可以序列化和反序列化的对象 他的类型也必须能够支持序列化和反序列化
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    Text k2 = new Text();
    LongWritable v2 = new LongWritable();

    @Override
    protected void map(LongWritable k1, Text v1, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
            throws IOException, InterruptedException {
        // step1:先获取k1和v1值
        long key = k1.get(); // 0
        String value = v1.toString(); // "hello tom"
        // step2:value里面究竟有几票呢?
        // 按空格切分value,分出单词
        String[] arr_value = value.split(" ");
        // arr_value[0] hello
        // arr_value[1] tom
        // step3 : 输出
        for (String word : arr_value) {
            k2.set(word);
            v2.set(1);
            context.write(k2, v2);
        }
    }
}