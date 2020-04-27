package cn.hut.rpc;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class RPCClient {
    public static void main(String[] args) {
        try {
            InetSocketAddress addr = new InetSocketAddress("192.168.88.1", 9527);
            Configuration conf = new Configuration();
            Bizable proxy = RPC.getProxy(Bizable.class, 10010, addr, conf);
            String res = proxy.sayHi("client123");
            System.out.println(res);
            RPC.stopProxy(proxy);

        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }
    // public static void main(String[] args) {
    //     RPCServer ser = new RPCServer();       
    //     System.out.println(ser.sayHi("client123"));
    // }
}