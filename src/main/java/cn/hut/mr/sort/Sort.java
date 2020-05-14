package cn.hut.mr.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Sort {
    // 2 k1=0,v1=2
    // 32
    // 654
    // 32
    // 15
    // 765
    // 65223
    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {

        private static IntWritable data = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString(); //2 
            data.set(Integer.parseInt(line)); //data =2
            context.write(data, new IntWritable(1)); // k2 = 2 v2=1
        }
    }

    // 2, 1
    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private static IntWritable linenum = new IntWritable(1);

        public void reduce(IntWritable k2, Iterable<IntWritable> v2s, Context context)
                throws IOException, InterruptedException {
            // key =2，values={1，1}
            for (IntWritable val : v2s) {
                context.write(linenum, k2); // 1，2 // 2，2
                linenum = new IntWritable(linenum.get() + 1);
            }
        }
    }
    // k2 ,v2 
    public static class Partition extends Partitioner<IntWritable, IntWritable> {
        @Override                         //2               //1 
        public int getPartition(IntWritable k2, IntWritable v2, int numPartitions) {

            int Maxnumber = 65223;
            int bound = Maxnumber / numPartitions + 1; //65224
            int keynumber = k2.get(); //2 
            for (int i = 0; i < numPartitions; i++) { // [-65224,0) 
                if (keynumber < bound * i && keynumber >= bound * (i - 1))
                    return i - 1; //-1
            }
            return -1;

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        String[] otherArgs = {"D:\\centos\\sortinput","D:\\centos\\sortout"}; // new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
          System.err.println("Usage: wordcount <in> <out>");
          System.exit(2);
        }
        Path outPath = new Path(otherArgs[1]);
        // 判断输出文件是否已经存在，存在的话就将其删除
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        Job job = new Job(conf, "Sort");
        job.setJarByClass(Sort.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(Partition.class);
        job.setNumReduceTasks(3);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
    

}