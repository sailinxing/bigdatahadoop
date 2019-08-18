package com.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class HdfsClientDemo {
    FileSystem fs = null;
    Configuration conf = null;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:9000");
        //拿到一个文件系统操作的客户端实例对象
        /*fs = FileSystem.get(conf);*/
        //可以直接传入 uri和用户身份
        fs = FileSystem.get(new URI("hdfs://node1:9000"), conf, "root"); //最后一个参数为用户名
    }

    @Test
    public void testUpload() throws Exception {

        Thread.sleep(2000);
        fs.copyFromLocalFile(new Path("d:/aaa.txt"), new Path("/access.log.copy.love"));
        fs.close();
    }

    @Test
    public void testDownload() throws Exception {

        fs.copyToLocalFile(new Path("/testdfs.txt"), new Path("d:/"));
        fs.close();
    }

    @Test
    public void testConf() {
        Iterator<Map.Entry<String, String>> iterator = conf.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            System.out.println(entry.getValue() + "--" + entry.getValue());//conf加载的内容
        }
    }

    /**
     * 创建目录
     */
    @Test
    public void makdirTest() throws Exception {
        boolean mkdirs = fs.mkdirs(new Path("/sss/bbb"));
        System.out.println(mkdirs);
    }

    /**
     * 删除
     */
    @Test
    public void deleteTest() throws Exception {
        boolean delete = fs.delete(new Path("/sss"), true);//true， 递归删除
        System.out.println(delete);
    }

    @Test
    public void listTest() throws Exception {

        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            System.err.println(fileStatus.getPath() + "=================" + fileStatus.toString());
            System.err.println(fileStatus.isFile() ? "file" : "directory");
        }
        //会递归找到所有的文件
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus next = listFiles.next();
            System.out.println("blocksize" + next.getBlockSize());
            System.out.println("Owner" + next.getOwner());
            System.out.println("Replication" + next.getReplication());
            System.out.println("Permission" + next.getPermission());
            String name = next.getPath().getName();
            Path path = next.getPath();
            System.out.println(name + "---" + path.toString());
            System.out.println("-----------------------");
            BlockLocation[] blockLocations = next.getBlockLocations();
            for (BlockLocation b : blockLocations) {
                System.out.println("块起始偏移量" + b.getOffset());
                System.out.println("块长度" + b.getLength());
                System.out.println("块位置datanode" + Arrays.toString(b.getHosts()));
            }

        }
    }

}
