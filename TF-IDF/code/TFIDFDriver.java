package ecp.Lab1.TFIDF;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
/**
 * WordFrequenceInDocument creates the index of the words in documents,
 * mapping each of them to their frequency.
 */
public class TFIDFDriver extends Configured implements Tool {
 
    // Folder where the output goes
    private static final String OUTPUT_PATH = "3-tf-idf";
 
    // Folder which contains the input data
    private static final String INPUT_PATH = "2-word-count";
 
    public int run(String[] args) throws Exception {
 
        Configuration conf = getConf();
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "Word in Corpus, TF-IDF");
 
        job.setJarByClass(TFIDFDriver.class);
        job.setMapperClass(TFIDFMapper.class);
        job.setReducerClass(TFIDFReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
 
        // Getting the number of documents from the original input directory.
        Path inputPath = new Path("input");
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] stat = fs.listStatus(inputPath);
 
        // Dirty hack to pass the total number of documents as the job name.
        job.setJobName(String.valueOf(stat.length));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TFIDFDriver(), args);
        System.exit(res);
    }
}
