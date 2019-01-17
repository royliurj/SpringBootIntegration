package com.roy.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Roy
 * @Date: 2019/1/17 14:20
 */
public class ZKClient {
    private String connectString = "192.168.50.232:2181,192.168.50.231:2181,192.168.50.230:2181";
    private int timeout = 2000;
    private ZooKeeper zkClient;

    //获取连接
    public void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, timeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    getServerList();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZKClient client = new ZKClient();
        //1. 获取连接
        client.getConnect();

        //2. 监听服务器节点路径
        client.getServerList();

        //3. 业务处理
        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * 获取服务器列表
     */
    private void getServerList() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/servers", true);

        List<String> serverList = new ArrayList<>();

        for (String child: children) {
            byte[] data = zkClient.getData("/servers/" + child, false, null);
            serverList.add(new String(data));
        }

        System.out.println(serverList);
    }
}
