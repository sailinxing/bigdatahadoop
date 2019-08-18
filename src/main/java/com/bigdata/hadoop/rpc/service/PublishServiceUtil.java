package com.bigdata.hadoop.rpc.service;

import com.bigdata.hadoop.rpc.protocol.ClientNamenodeProtocol;
import com.bigdata.hadoop.rpc.protocol.IUserLoginService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;

import java.io.IOException;

public class PublishServiceUtil {

    public static void main(String[] args) throws IOException {
        //第一个rpc
        Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress("localhost")
                .setPort(8888)
                .setProtocol(ClientNamenodeProtocol.class)
                .setInstance(new MyNameNode());

        Server server = builder.build();
        server.start();

        //第二个登录rpc
        Builder builder2 = new RPC.Builder(new Configuration());
        builder2.setBindAddress("localhost")
                .setPort(9999)
                .setProtocol(IUserLoginService.class)
                .setInstance(new UserLoginServiceImpl());

        Server server2 = builder2.build();
        server2.start();


    }
}
