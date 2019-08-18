package com.bigdata.hadoop.rpc.service;

import com.bigdata.hadoop.rpc.protocol.ClientNamenodeProtocol;

public class MyNameNode implements ClientNamenodeProtocol {
    //模拟namenode的业务方法之一：查询元数据     第一个rpc server
    @Override
    public String getMetaData(String path) {

        return path + ": 3 - {BLK_1,BLK_2} ....";

    }
}
