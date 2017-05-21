package ecp.Lab1.WordFrequency;

import java.io.IOException;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
/**
 * LineIndexReducer Takes a list of filename@offset entries for a single word and concatenates them into a list.
 */
public class WordFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
 
    public WordFrequencyReducer() {
    }
 
    /**
     * @param key is the key of the mapper
     * @param values are all the values aggregated during the mapping phase
     * @param context contains the context of the job run
     *
     *      PRE-CONDITION: receive a list of <"word@filename",[1, 1, 1, ...]> pairs
     *        <"salut@coucou.txt", [1, 1]>
     *
     *      POST-CONDITION: emit the output a single key-value where the sum of the occurrences.
     *        <"salut@coucou.txt", 2>
     */
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
 
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        // Write the key and the adjusted value (removing the last comma)
        context.write(key, new IntWritable(sum));
    }
}