package ecp.Lab1.WordCount;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 

public class WordCountDriver extends Configured implements Tool {
 
    // where to put the data in hdfs when we're done
    private static final String OUTPUT_PATH = "2-word-count";
 
    // where to read the data from.
    private static final String INPUT_PATH = "1-word-freq";
 
    public int run(String[] args) throws Exception {
 
        Configuration conf = getConf();
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "WordCount");
 
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class); 
 
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCountDriver(), args);
        System.exit(res);
    }
}
	