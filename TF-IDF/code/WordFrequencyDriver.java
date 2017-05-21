package ecp.Lab1.WordFrequency;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
/**
 * WordFrequenceInDocument Creates the index of the words in documents,
 * mapping each of them to their frequency.
 */
public class WordFrequencyDriver extends Configured implements Tool {
 
    // Folder in hdfs where the output goes
    private static final String OUTPUT_PATH = "1-word-freq";
 
    // Folder which contains the input data
    private static final String INPUT_PATH = "input";
 
    public int run(String[] args) throws Exception {
 
        Configuration conf = getConf();
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "Word Frequence In Document");
 
        job.setJarByClass(WordFrequencyDriver.class);
        job.setMapperClass(WordFrequencyMapper.class);
        job.setReducerClass(WordFrequencyReducer.class);
        job.setCombinerClass(WordFrequencyReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordFrequencyDriver(), args);
        System.exit(res);
    }
}
