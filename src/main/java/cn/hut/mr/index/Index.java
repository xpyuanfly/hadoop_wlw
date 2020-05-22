package cn.hut.mr.index;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.hut.mr.utils.FileUtils;

public class Index {
    public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException,
            InterruptedException {
        Configuration conf = new Configuration();
        String argument0 = "D:\\centos\\inputIndex";
        String argument1 = "D:\\centos\\outputIndex";

        if (args.length == 2) {
            argument0 = args[0];
            argument1 = args[1];
        }

        File file = new File(argument1);
        if (file.exists()) {
            // file.delete();
            FileUtils.delFile(file);
        }

        Job job = new Job(conf, "Inverted Index");
        job.setJarByClass(Index.class);
        job.setMapperClass(IndexMapper.class);
        job.setCombinerClass(IndexCombiner.class);
        job.setReducerClass(IndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
       
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(argument0));
        FileOutputFormat.setOutputPath(job, new Path(argument1));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
    public static class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        private FileSplit split;
        private Text k2 = new Text();
        private Text v2 = new Text();

        @Override
        protected void map(LongWritable k1, Text v1, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            split = (FileSplit) context.getInputSplit();
            // Mapreduce is simple
            StringTokenizer itr = new StringTokenizer(v1.toString());
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken(); // MapReduce
                // D:\\centos\\inputIndex\file1.txt
                int beginIndex = split.getPath().toString().indexOf("fff");
                String filename = split.getPath().toString().substring(beginIndex); // file1.txt
                k2.set(token + ":" + filename);
                v2.set("1");
                context.write(k2, v2);// MapReduce:file1.txt,1
            }

        }
    }

    public static class IndexCombiner extends Reducer<Text, Text, Text, Text> {
        private Text v3 = new Text();
        private Text k3 = new Text();

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            // 统计词频
            int sum = 0;
            for (Text v2 : v2s) {
                sum += Integer.parseInt(v2.toString());
            }
            // MapReduce:file1.txt
            int splitIdx = k2.toString().indexOf(":");
            String token = k2.toString().substring(0, splitIdx);
            String filename = k2.toString().substring(splitIdx+1);

            k3.set(token);

            v3.set(filename + ":" + sum);
            context.write(k3, v3);
        }
    }

    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {

        private Text v3 = new Text();

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            // MapReduce {file3.txt:2;file2.txt:1;file1.txt:1}
            StringBuilder sb = new StringBuilder();
            for(Text v2 : v2s){
                sb.append(v2.toString()+";");
            }

            v3.set(sb.toString());
            context.write(k2, v3);

        }
    }
}