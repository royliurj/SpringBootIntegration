package com.roy.zk;

import com.sun.deploy.util.StringUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.List;

/**
 * @Author: Roy
 * @Date: 2019/1/17 10:37
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class ZkTest {

    //连接其中一个即可
    //private String connectString = "192.168.50.232:2181";
    private String connectString = "192.168.50.232:2181,192.168.50.231:2181,192.168.50.230:2181";
    //连接超时时间
    private int timeout = 2000;
    ZooKeeper zkClient;

    /**
     * 实例化zookeeper客户端
     * @throws IOException
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Before
    @Test
    public void contextLoads() throws IOException, KeeperException, InterruptedException {

        //创建一个zookeeperClient
        zkClient = new ZooKeeper(connectString, timeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

                //目的是为了继续监听，因为默认监听一次就结束了
                try {
                    zkClient.getChildren("/", true);
                    zkClient.exists("/node2", true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("type:"+watchedEvent.getType() + "\t" + "path:"+watchedEvent.getPath() + "\t" + "state:"+watchedEvent.getState() + "\t" + "wrapper:"+watchedEvent.getWrapper());
            }
        });
    }

    @Test
    public void create() throws KeeperException, InterruptedException {

        //创建节点
        zkClient.create("/node1", "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * 获取子节点
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void getChild() throws KeeperException, InterruptedException {
        //获取子节点
        //是否监听如果为true的话，需要当前线程不能关闭，调用Thread.sleep(1111)继续监听
        //如果监听属性为true的话，会调用创建zookeeper时的监听process方法
        List<String> children = zkClient.getChildren("/", true);
        System.out.println(StringUtils.join(children, ","));

        for (String child : children) {
            System.out.println(child);
        }

        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * 获取子节点
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void getChildren2() throws KeeperException, InterruptedException {
//        List<String> children = zkClient.getChildren("/node1", new Watcher() {
//            @Override
//            public void process(WatchedEvent watchedEvent) {
//                System.out.println("Node1 Changed");
//            }
//        });

        List<String> children = zkClient.getChildren("/node1", true);

        System.out.println(StringUtils.join(children, ","));
        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * 判断节点是否存在
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void exist() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/node2", true);
        System.out.println(stat);
        System.out.println(stat == null ? "not exist" : "exist");

        Thread.sleep(Integer.MAX_VALUE);
    }

//    @Test
//    public void delete() throws KeeperException, InterruptedException {
//        zkClient.delete("/node2", 1);
//    }

    /**
     * 获取节点值
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void getData() throws KeeperException, InterruptedException {
        byte[] data = zkClient.getData("/node2", false, null);
        System.out.println(new String(data));
    }
}
