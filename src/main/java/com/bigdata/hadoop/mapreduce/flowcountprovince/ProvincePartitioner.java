package com.bigdata.hadoop.mapreduce.flowcountprovince;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * K2  V2  对应的是map输出kv的类型
 *
 * @author
 */

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    public static HashMap<String, Integer> provinceDict = new HashMap<>();

    static {
        provinceDict.put("136", 0);
        provinceDict.put("137", 1);
        provinceDict.put("138", 2);
        provinceDict.put("139", 3);
    }


    @Override
    public int getPartition(Text key, FlowBean flowBean, int numPartitions) {
        String prefix = key.toString().substring(0, 3);
        Integer provinceId = provinceDict.get(prefix);
        return provinceId == null ? 4 : provinceId;
    }
}
