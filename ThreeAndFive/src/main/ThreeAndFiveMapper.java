package main;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ThreeAndFiveMapper  extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String cleanLine = value.toString().toLowerCase().replaceAll("[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']", " ");
        StringTokenizer itr = new StringTokenizer(cleanLine);
        
        while (itr.hasMoreTokens()) {
            String word = itr.nextToken().trim();
            if(word.length() == 3) {
            	context.write(new Text("3"), new IntWritable(1));
            }
            else if(word.length() == 5){
            	context.write(new Text("5"), new IntWritable(1));
            }
        }
        
    }
}
