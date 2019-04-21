package com.bigdata.hadoop.mapreduce.rightjoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * 订单表和商品表合到一起
 order.txt(订单id, 日期, 商品编号, 数量)
 1001	20150710	P0001	2
 1002	20150710	P0001	3
 1002	20150710	P0002	3
 1003	20150710	P0003	3
 product.txt(商品编号, 商品名字, 价格, 数量)
 P0001	小米5	1001	2
 P0002	锤子T1	1000	3
 P0003	锤子	1002	4
 */

public class RrightJoinLocal {
    static InfoBean bean=new InfoBean();
    static Text k=new Text();
    static class RrightJoinMapper extends Mapper<LongWritable,Text,Text,InfoBean>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            FileSplit inputSplit= (FileSplit) context.getInputSplit();
            // 通过文件名判断是哪种数据
            String pid = "";
            String name = inputSplit.getPath().getName();
            if (name.startsWith("order")) {
                String[] fields = line.split(",");
                // id date pid amount
                pid = fields[2];
                bean.set(Integer.parseInt(fields[0]), fields[1], pid, Integer.parseInt(fields[3]), "", 0, 0, "0");

            } else {
                String[] fields = line.split(",");
                // id pname category_id price
                pid = fields[0];
                bean.set(0, "", pid, 0, fields[1], Integer.parseInt(fields[2]), Float.parseFloat(fields[3]), "1");

            }
            k.set(pid);
            context.write(k,bean);

        }
    }
    static class RrightReducer extends Reducer<Text,InfoBean,InfoBean,NullWritable>{

        @Override
        protected void reduce(Text key, Iterable<InfoBean> values, Context context) throws IOException, InterruptedException {
            InfoBean pdBean=new InfoBean();
            ArrayList<InfoBean> orderBeans=new ArrayList<>();
            for(InfoBean bean:values){
                if("1".equals(bean.getFlag())){//产品的
                    try {
                        BeanUtils.copyProperties(pdBean,bean);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }
                else {
                    InfoBean odbean=new InfoBean();
                    try {
                        BeanUtils.copyProperties(odbean,bean);
                        orderBeans.add(odbean);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }
            }
            // 拼接两类数据形成最终结果
            for(InfoBean bean:orderBeans){
                bean.setPname(pdBean.getPname());
                bean.setCategory_id(pdBean.getCategory_id());
                bean.setPrice(pdBean.getPrice());
                context.write(bean,NullWritable.get());
            }
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        //是否运行为本地模式，就是看这个参数值是否为local，默认就是local
        conf.set("mapreduce.framework.name", "local");

        conf.set("mapred.textoutputformat.separator", ",");

        //本地模式运行mr程序时，输入输出的数据可以在本地，也可以在hdfs上
        //到底在哪里，就看以下两行配置你用哪行，默认就是file:///
		/*conf.set("fs.defaultFS", "hdfs://192.168.9.113:9000/");*/
        conf.set("fs.defaultFS", "file:///");


        //运行集群模式，就是把程序提交到yarn中去运行
        //要想运行为集群模式，以下3个参数要指定为集群上的值
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.hostname", "192.168.9.113");
//		conf.set("fs.defaultFS", "hdfs://192.168.9.113:9000/");

        Job job = Job.getInstance(conf);

        /*job.setJar("c:/wc.jar");*/
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(RrightJoinLocal.class);

        // 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(RrightJoinMapper.class);
        job.setReducerClass(RrightReducer.class);
        // 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);

        // 指定最终输出的数据的kv类型
        job.setOutputKeyClass(InfoBean.class);
        job.setOutputValueClass(NullWritable.class);



        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);



    }

}
