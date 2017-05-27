/**
 * Created by gpf on 17-5-27.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsOperate {

    static {
        System.out.println("Hello world!");
    }

    static Configuration conf = new Configuration();
    static {
        conf.set("fs.defaultFS", "hdfs://node1:9000");
    }

    public static void uploadFileToHdfs(String filePath, String dst) throws Exception {
        FileSystem hdfs = FileSystem.get(conf);
        Path srcPath = new Path(filePath);
        Path dstPath = new Path(dst);

        long start = System.currentTimeMillis();
        hdfs.copyFromLocalFile(false, srcPath, dstPath);

        System.out.println("Time:" + (System.currentTimeMillis() - start));
        System.out.println("_________________Upload to "+conf.get("fs.defaultFS") + "________________");

        Path getPath = new Path("/user");
        long usedSize = hdfs.getUsed(getPath);
        System.out.println("hdfs used size : " + Long.toString(usedSize));

        hdfs.close();
    }


    public static void main(String[] args) throws Exception {

        System.out.println("args[0] : " + args[0]);
        System.out.println("args[1] : " + args[1]);

        try {
            uploadFileToHdfs(args[0], args[1]);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}

