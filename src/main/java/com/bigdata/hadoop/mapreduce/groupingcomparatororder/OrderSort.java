package com.bigdata.hadoop.mapreduce.groupingcomparatororder;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderSort {
    static class OrderSortMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
        OrderBean bean = new OrderBean();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = StringUtils.split(line, ",");
            bean.set(new Text(fields[0]),new DoubleWritable(Double.parseDouble(fields[1])));
            context.write(bean,NullWritable.get());
        }
    }
    static class OrderSortReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {


        //到达reduce时，相同id的所有bean已经被看成一组，且金额最大的那个一排在第一位
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(OrderSort.class);
        job.setMapperClass(OrderSortMapper.class);
        job.setReducerClass(OrderSortReducer.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("D:/testhadoopdata/input/ordergroup"));
        FileOutputFormat.setOutputPath(job, new Path("D:/testhadoopdata/output/ordergroup"));
        //在此设置自定义的Groupingcomparator类
        job.setGroupingComparatorClass(ItemidGroupingComparator.class);
        //在此设置自定义的partitioner类
        job.setPartitionerClass(ItemIdPartitioner.class);
        job.setNumReduceTasks(2);
        job.waitForCompletion(true);
    }
    }
