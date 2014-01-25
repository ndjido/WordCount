/*
 *@author Ndjido A BAR ndjido@gmail.com
 *
 *@tested on Hadoop 1.2.1
 */

package wordcount;

//java core packages
import java.io.*;
import java.util.*;

// hadoop packages
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 *
 * @author ndjido
 */
public class WordCount extends Configured implements Tool{

    /**
     * @brief Mapper class
     */
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        //member variable
        private final Text word = new Text();
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while(tokenizer.hasMoreTokens())
            {
                word.set(tokenizer.nextToken());
                context.write(word, new IntWritable(1));
            }
            
        }
          
    }
    
    /**
     * @brief Reducer call
     */
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> itval, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            Iterator<IntWritable> values = itval.iterator();
            while(values.hasNext())
            {
                sum += values.next().get();
            }
            
            context.write(key, new IntWritable(sum));
               
        }
        
    }
    
    /**
     * @brief Run method
     * @param args
     * @return
     * @throws Exception 
     */
    @Override
    public int run(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "WordCount");
        
        // Set up the input
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        //Set up mapper
        job.setMapperClass(Map.class);
        
        //Set up reducer
        job.setReducerClass(Reduce.class);
        
        //Set up output
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        
        //Run        
        return (job.waitForCompletion(true)) ? 0 : -1;
        
    }
    
    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args)  throws Exception{
        // Running mapred job
        int result = ToolRunner.run(new WordCount(), args);
        System.exit(result);
    }
    
}
