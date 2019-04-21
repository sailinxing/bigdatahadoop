package com.bigdata.hadoop.mapreduce.fans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

public class SharedFriendsStepTwo {
    static class SharedFriendsStepTwoMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fans_persons = line.split("\t");
            String fans=fans_persons[0];
            String persons=fans_persons[1];
            String[] pers=persons.split(",");
            Arrays.sort(pers);
            for(int i=0;i<pers.length-1;i++){
                for(int j=i+1;j<pers.length;j++){
                    // 发出 <人-人，好友> ，这样，相同的“人-人”对的所有好友就会到同1个reduce中去
                    context.write(new Text(pers[i]+"-"+pers[j]),new Text(fans));
                }
            }
        }
    }
    static class SharedFriendsStepTwoReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb=new StringBuffer();
            for(Text fan:values){
                sb.append(fan).append(" ");
            }
            context.write(key,new Text(sb.toString()));
        }
    }
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(SharedFriendsStepTwo.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SharedFriendsStepTwoMapper.class);
        job.setReducerClass(SharedFriendsStepTwoReducer.class);

        FileInputFormat.setInputPaths(job, new Path("D:/testhadoopdata/output/fansstepone/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("D:/testhadoopdata/output/fanssteptwo"));

        job.waitForCompletion(true);

    }
}
