package cn.hut.mr.friends;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class FindCommonFriendsStep1 {

    public static class FindCommonFriendsStep1Mapper extends Mapper<LongWritable,Text,Text,Text>{

        //tom		cat	hadoop	hello
        @Override
        protected void map(LongWritable k1, Text v1, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String line = v1.toString();
            String [] line_split = line.split("\t");
            String user = line_split[0];

            for(int i=1;i<line_split.length;i++){
                context.write(new Text(user), new Text(line_split[i]));
            }

            // tom cat 
            // tom hadoop
            // tom hello           
        }        
    }

    public static class FindCommonFriendsStep1Reducer extends Reducer<Text,Text,Text,Text>{

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
           
        }
    }
    
}