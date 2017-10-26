package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class ThreeAndFive {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		new ThreeAndFive(args);

	}
	
	public ThreeAndFive(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		FileSystem fs = FileSystem.newInstance(conf);
        if (otherArgs.length != 2) {
            System.err.println("Usage: TopN [in] [out]");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("Top N");
        job.setJarByClass(ThreeAndFive.class);
        
        job.setMapperClass(ThreeAndFiveMapper.class);
        
        //Uncomment this for faster reducer
        //job.setCombinerClass(ThreeAndFiveReducer.class);
        job.setReducerClass(ThreeAndFiveReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        Path outputPath = new Path(otherArgs[1]);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, outputPath);
        
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
