
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

public class WordCount {

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

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        try {
            uploadFileToHdfs("/home/gpf/test_work/zhuanli-test.txt", "/user/cluster");
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
