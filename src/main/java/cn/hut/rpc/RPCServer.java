package cn.hut.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;

public class RPCServer implements Bizable {

    @Override
    public String sayHi(String name) {
        return "hello~~" + name;
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Builder builder = new Builder(conf);
            builder.setBindAddress("192.168.88.1").setPort(9527).setProtocol(Bizable.class).setInstance(new RPCServer()); // 链式调用

            Server server = builder.build();
            server.start();
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }

    }
}