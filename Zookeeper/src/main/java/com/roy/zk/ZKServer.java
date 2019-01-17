package com.roy.zk;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @Author: Roy
 * @Date: 2019/1/17 13:57
 */
public class ZKServer {

    private String connectString = "192.168.50.232:2181,192.168.50.231:2181,192.168.50.230:2181";
    private int timeout = 2000;
    private ZooKeeper zkClient;
    private String parentNode = "/servers";

    //获取连接
    public void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, timeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZKServer server = new ZKServer();
        //1. 获取连接zkServer
        server.getConnect();

        //2. 注册服务器节点信息
        server.regist(args[0]);

        //3. 具提业务逻辑处理
        System.out.println(args[0] + " is online!");

        Thread.sleep(Integer.MAX_VALUE);
    }

    //注册服务器节点
    private void regist(String ip) throws KeeperException, InterruptedException {
        //创建节点类型，必须是临时的带序号的
        String path = zkClient.create(parentNode + "/server", ip.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("path:" + path);
    }
}
