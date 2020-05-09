package cn.hut.hdfs_demo.mr.friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FindCommonFriendStep2 {
    public static class Step2Mapper extends Mapper<LongWritable, Text, Text, Text> {
        Text k2 = new Text();
        Text v2 = new Text();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            //0 cat-hadoop    Tom
            //17    cat-hello Tom
            //30    hadoop-hello  Tom
            //...
            String[] line_split = v1.toString().split("\t");
            k2.set(line_split[0]);
            v2.set(line_split[1]);
            context.write(k2, v2);
            //cat-hadoop    Tom
            //cat-hello Tom
            //hadoop-hello  Tom
            //...
        }
    }

    public static class Step2Reducer extends Reducer<Text, Text, Text, IntWritable> {
        IntWritable v3 = new IntWritable();

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            //Tom-hive  {cat,hadoop,hello}
            //...
            int count = 0;
            while (v2s.iterator().hasNext()) {
                count ++;
                v2s.iterator().next();
            }
            v3.set(count);
            context.write(k2, v3);
            //Tom-hive  3
            //...
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);

            job.setJarByClass(FindCommonFriendStep2.class);
            job.setMapperClass(Step2Mapper.class);
            job.setReducerClass(Step2Reducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.setInputPaths(job, new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\共同好友问题\\output"));
            Path outPath = new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\共同好友问题\\sameFriendsNumber");
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }
            FileOutputFormat.setOutputPath(job, outPath);

            System.out.println("推荐好友步骤二执行" + (job.waitForCompletion(true) ? "成功" : "失败"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
