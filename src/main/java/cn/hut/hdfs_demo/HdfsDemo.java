package cn.hut.hdfs_demo;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import cn.hut.hdfs_demo.utils.TimeUtil;

public class HdfsDemo {

    FileSystem fs = null;

    // public void FileSystem(){
    // this.init();
    // }

    public void init() {
        try {
            final String str = "hdfs://itcast01:9000";
            final URI uri = new URI(str);
            final Configuration conf = new Configuration();
            final String user = "root";
            fs = FileSystem.get(uri, conf, user);
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    /// goal: hello/hello => c:/hello_get
    public void copyToLocalFile() {
        try {
            final Path src = new Path("/hello/hello");
            final Path dst = new Path("c:/hello_CopyToLocalFile");
            fs.copyToLocalFile(src, dst);
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    /// goal: hello/hello => c:/hello_2stream
    public void get2Stream() {
        try {
            /// step2 ： 获取文件/hello/hello的输入流
            final int bufferSize = 4096;
            final Path f = new Path("/hello/hello");
            final InputStream input = fs.open(f, bufferSize);

            /// step3 : 获取文件的输出流
            final String file = "c://hello_Get2Stream";
            final FileOutputStream output = new FileOutputStream(file, true);

            /// step4 : 将两个流连接在一起
            IOUtils.copy(input, output);

            /// step5 : 关闭2个流
            input.close();
            output.close();

        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    /// goal: c:/hello_2strea m=> hello/hello_upload_2Stream
    public void put2Stream() {
        try {
            /// step1 ： 获取一个filesystem
            final String str = "hdfs://itcast01:9000";
            final URI uri = new URI(str);
            final Configuration conf = new Configuration();
            final String user = "root";
            final FileSystem fs = FileSystem.get(uri, conf, user);

            /// step2： 获取input流
            final String file = "c:/hello_upload";
            final FileInputStream input = new FileInputStream(file);

            // step3 : 获取output流
            final Path f = new Path("/hello/hello_upload");
            final OutputStream output = fs.create(f);

            // step4: 拼接两个流
            IOUtils.copy(input, output);

            // step5 : 两个流关闭
            input.close();
            output.close();

        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    /// goal :Name，Type, Size，Replication，Block Size，Modification
    /// Time，Permission，Owner，Group
    public void ls() throws Exception {
        final RemoteIterator<LocatedFileStatus> ri = fs.listFiles(new Path("/hello"), false);
        while (ri.hasNext()) {
            String content = "";
            final LocatedFileStatus lfs = ri.next();
            content += lfs.getPath().getName() + "|";
            content += lfs.isDirectory() ? "dir" : "file" + "|";
            content += lfs.getLen() + "|";
            content += lfs.getReplication() + "|";
            content += lfs.getBlockSize() + "|";
            content += TimeUtil.longToDateString(lfs.getModificationTime()) + "|";
            content += lfs.getPermission() + "|";
            content += lfs.getOwner() + "|";
            content += lfs.getGroup();

            System.out.println(content);
        }
    }

    public void rm() throws Exception {
        fs.delete(new Path("/hello/"), true);
    }

    public static void main(final String[] args) {
        System.out.println("--- start hdfs demo ----");
        final HdfsDemo hd = new HdfsDemo();
        try {
            hd.init();
            hd.get2Stream();
            // hd.test_Put2Stream();
            // hd.test_CopyToLocalFile();
            // hd.test_ls();
            // hd.rm();
        } catch (final Exception e) {
            e.printStackTrace();
        }

        System.out.println("--- success finish ----");

    }

}