package ecp.Lab1.PageRank;

import ecp.Lab1.PageRank.PageRank;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
public class VertexMapper extends Mapper<LongWritable,Text,Text,Text>{

	@Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		
		if (value.charAt(0) != '#') {
		int index = value.find("\t");
        String vertex1 = Text.decode(value.getBytes(), 0, index);
        String vertex2= Text.decode(value.getBytes(), index + 1, value.getLength() - (index + 1));
        context.write(new Text(vertex1), new Text(vertex2));
        
        PageRank.LIST.add(vertex1);
        PageRank.LIST.add(vertex2);
        
    }

	}
	}
	