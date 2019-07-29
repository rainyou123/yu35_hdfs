import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class IOHdfs {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        // 1 创建配置信息对象
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:8020"),configuration, "root");

        // 2 创建输入流
        FileInputStream inStream = new FileInputStream(new File("E:/hello.txt"));

        // 3 创建输出流
        FSDataOutputStream outStream = fs.create(new Path("/user/weichuang/hello1.txt"));

        // 4 流对接
        try{
            IOUtils.copyBytes(inStream, outStream, configuration);
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            //5关闭
            IOUtils.closeStream(inStream);
            IOUtils.closeStream(outStream);
        }

    }
    @Test
    public void seek1() throws URISyntaxException, IOException, InterruptedException {
        // 1 创建配置信息对象
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:8020"),configuration, "root");

        // 2创建输出流
        FileOutputStream outStream = new FileOutputStream(new File("e:/hello.txt"));

        // 3 创建输入流
        FSDataInputStream inStream = fs.open(new Path("/user/weichuang/hello1.txt"));

        // 4 流对接输出到控制台
        try{
            IOUtils.copyBytes(inStream, outStream , configuration );
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            IOUtils.closeStream(inStream);
            IOUtils.closeStream(outStream );
        }

    }
}
