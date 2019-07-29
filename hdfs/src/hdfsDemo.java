import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class hdfsDemo {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop100:8020"), configuration, "root");
        fileSystem.copyFromLocalFile(new Path("E:/1.txt"), new Path("/user/weichuang/input"));
        fileSystem.close();
    }
}
