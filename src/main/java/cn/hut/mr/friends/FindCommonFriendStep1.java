package cn.hut.hdfs_demo.mr.friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class FindCommonFriendStep1 {

    public static class Step1Mapper extends Mapper<LongWritable, Text, Text, Text> {
        Text k2 = new Text();
        Text v2 = new Text();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            //0 Tom	cat	hadoop	hello
            //...
            String[] line_split = v1.toString().split("\t");

            k2.set(line_split[0]);
            for (int i = 1; i < line_split.length; i ++) {
                v2.set(line_split[i]);
                context.write(k2, v2);
            }
            //Tom	cat
            //Tom	hadoop
            //Tom	hello
            //...
        }
    }

    public static class Step1Reducer extends Reducer<Text, Text, Text, Text> {
        Text k3 = new Text();
        ArrayList<String> arr_v2s;

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            //Tom	{cat	hadoop	hello}
            //...
            arr_v2s = new ArrayList<>();
            String s1, s2;

            //将v2s中的元素放到数组中
            for (Text v2 : v2s) {
                arr_v2s.add(v2.toString());
                System.out.println(k2.toString() + ":" + v2.toString());
            }

            int length = arr_v2s.size();
            for (int i = 0; i < length - 1; i ++) {
                for (int j = i + 1; j < length; j ++) {
                    s1 = arr_v2s.get(i);
                    s2 = arr_v2s.get(j);

                     //对于cat-hadoop还是hadoop-cat，都将按cat-hadoop组合
                    if (s1.compareTo(s2) < 0) {
                        k3.set(s1 + "-" + s2);
                    }
                    else {
                        k3.set(s2 + "-" + s1);
                    }
                    context.write(k3, k2);
                }
            }
            //cat-hadoop    Tom
            //cat-hello Tom
            //hadoop-hello  Tom
            //...
            arr_v2s = null;
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);

            job.setJarByClass(FindCommonFriendStep1.class);
            job.setMapperClass(Step1Mapper.class);
            job.setReducerClass(Step1Reducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job, new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\共同好友问题\\input\\推荐好友"));
            Path outPath = new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\共同好友问题\\output");
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }
            FileOutputFormat.setOutputPath(job, outPath);

            System.out.println("推荐好友步骤一执行" + (job.waitForCompletion(true) ? "成功" : "失败"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
