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

public class SharedFriendsStepOne {
    static class SharedFriendStepOneMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // A:B,C,D,F,E,O
            String line = value.toString();
            String[] person_fans = line.split(":");
            String person = person_fans[0];
            String fans = person_fans[1];
            String[] fanss = fans.split(",");
            for (String fan : fanss) {
                // 输出<好友，人>
                context.write(new Text(fan), new Text(person));
            }

        }

    }

    static class SharedFriendsStepOneReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> persons, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for (Text person : persons) {
                sb.append(person).append(",");
            }
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(SharedFriendsStepOne.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SharedFriendStepOneMapper.class);
        job.setReducerClass(SharedFriendsStepOneReducer.class);

        FileInputFormat.setInputPaths(job, new Path("D:/testhadoopdata/input/fans"));
        FileOutputFormat.setOutputPath(job, new Path("D:/testhadoopdata/output/fansstepone"));

        job.waitForCompletion(true);

    }
}
