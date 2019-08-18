package com.bigdata.hadoop.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 用流的方式来操作hdfs上的文件
 * 可以实现读取指定偏移量范围的数据
 *
 * @author
 */
public class HdfsStreamAccess {
    FileSystem fs = null;
    Configuration conf = null;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        conf = new Configuration();
        //拿到一个文件系统操作的客户端实例对象
        //可以直接传入 uri和用户身份
        fs = FileSystem.get(new URI("hdfs://node1:9000"), conf, "root");

    }

    /**
     * 通过流的方式上传文件到hdfs
     *
     * @throws Exception
     */
    @Test
    public void testUpload() throws IOException {
        FSDataOutputStream outputStream = fs.create(new Path("/angela.love"), true);
        FileInputStream inputStream = new FileInputStream("d:/aaa.txt");
        IOUtils.copy(inputStream, outputStream);
    }

    /**
     * 通过流的方式获取hdfs上数据
     *
     * @throws Exception
     */
    @Test
    public void testDownLoad() throws IOException {
        FSDataInputStream inputStream = fs.open(new Path("/testdfs.txt"));
        FileOutputStream outputStream = new FileOutputStream("d:/aaa.txt");
        IOUtils.copy(inputStream, outputStream);
    }

    /**
     * 获取指定位置数据
     *
     * @throws Exception
     */
    @Test
    public void testRandomAccess() throws Exception {

        FSDataInputStream inputStream = fs.open(new Path("/testdfs.txt"));

        inputStream.seek(4);

        FileOutputStream outputStream = new FileOutputStream("d:/angelababy.love.part3");

        IOUtils.copy(inputStream, outputStream);
    }

    /**
     * 显示hdfs上文件的内容
     *
     * @throws IOException
     * @throws IllegalArgumentException
     */
    @Test
    public void testCat() throws IOException {
        FSDataInputStream in = fs.open(new Path("/testdfs.txt"));
        IOUtils.copy(in, System.out);
    }
}
