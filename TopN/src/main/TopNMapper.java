package main;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNMapper  extends Mapper<Object, Text, Text, IntWritable> {
	private Map<String, Integer> countMap = new HashMap<>();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String cleanLine = value.toString().toLowerCase().replaceAll("[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']", " ");
        StringTokenizer itr = new StringTokenizer(cleanLine);
        
        while (itr.hasMoreTokens()) {
            String word = itr.nextToken().trim();
            if (countMap.containsKey(word)) {
                countMap.put(word, countMap.get(word)+1);
            } else {
                countMap.put(word, 1);
            }
        }
        
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String key: countMap.keySet()) {
            context.write(new Text(key), new IntWritable(countMap.get(key)));
        }
    }
}
