package ecp.Lab1.WordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 

public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> { 
 
    public WordCountMapper() {
    }
 
    /**
     * @param key is the byte offset of the current line in the file;
     * @param value is the line from the file
     * @param context
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] wordAndDocCounter = value.toString().split("\t");
        String[] wordAndDoc = wordAndDocCounter[0].split("@");
        context.write(new Text(wordAndDoc[1]), new Text(wordAndDoc[0] + "=" + wordAndDocCounter[1]));
    }
}