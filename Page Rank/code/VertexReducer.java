package ecp.Lab1.PageRank;

import ecp.Lab1.PageRank.PageRank;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VertexReducer extends Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
    
        
        boolean first = true;
        String outranks= (PageRank.DAMPING / PageRank.LIST.size()) + "\t";

        for (Text value : values) {
            if (!first) 
                outranks += ",";
            outranks += value.toString();
            first = false;
        }

        context.write(key, new Text(outranks));
    }

}