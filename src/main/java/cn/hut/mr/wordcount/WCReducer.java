package cn.hut.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable count = new LongWritable();
    int k = 3;
    int i = 0;

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        if (i >= 3)
            return;

        long sum = 0;

        for (LongWritable value : values) {

            long i = value.get();
            sum = sum + i;
        }
        count.set(sum);

        context.write(key, count);

        i++;
    }
}