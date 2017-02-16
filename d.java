package org.myorg;
        
import java.io.IOException;
import java.util.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;
        
public class InvIndex {
        
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private boolean caseSensitive = false;
    private long numRecords = 0;
    private String input;
    private Set<String> patternsToSkip = new HashSet<String>();

    protected void setup(Mapper.Context context)
        throws IOException,
        InterruptedException {
      if (context.getInputSplit() instanceof FileSplit) {
        this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
      } else {
        this.input = context.getInputSplit().toString();
      }
      Configuration config = context.getConfiguration();
      this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
      if (config.getBoolean("wordcount.skip.patterns", false)) {
        URI[] localPaths = context.getCacheFiles();
        parseSkipFile(localPaths[0]);
      }
    }

    private void parseSkipFile(URI patternsURI) {
      try {
        BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
        String pattern;
        while ((pattern = fis.readLine()) != null) {
          patternsToSkip.add(pattern);
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file '"
            + patternsURI + "' : " + StringUtils.stringifyException(ioe));
      }
    }
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line, " \t\n\r\f,.:;?![]{}\"'()~_-");
        String filename = ((FileSplit) context.getInputSplit()).getPath().getName();

        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken().toLowerCase();
            if (word.isEmpty() || patternsToSkip.contains(word)) {
                continue;
            }
            context.write(new Text(word), new Text(filename));
        }
    }
 } 

 public static class Combine extends Reducer<Text, Text, Text, java.util.HashMap<String,IntWritable>> {

    public void combine(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        java.util.HashMap<String,IntWritable> dict = new java.util.HashMap<String,IntWritable>();

        dict.put("pg100.txt", new IntWritable(0));
        dict.put("pg3200.txt", new IntWritable(0));
        dict.put("pg33100.txt", new IntWritable(0));

        for (Text val : values) {
            dict.put(val.toString(), new IntWritable(dict.get(val).get() + 1));
        }

        context.write(key, dict);
    }
 }
        
 public static class Reduce extends Reducer<Text, java.util.HashMap<String,IntWritable>, Text, Text> {

    public void reduce(Text key, Iterable<java.util.HashMap<String,IntWritable>> values, Context context) 
      throws IOException, InterruptedException {

        java.util.HashMap<String,IntWritable> dict = new java.util.HashMap<String,IntWritable>();
        dict.put("pg100.txt", new IntWritable(0));
        dict.put("pg3200.txt", new IntWritable(0));
        dict.put("pg33100.txt", new IntWritable(0));

        String files = new String();

        List<String> fnames = new ArrayList<String>();
            fnames.add("pg100.txt");
            fnames.add("pg3200.txt");
            fnames.add("pg33100.txt");

        for (String file : fnames ) {
            int sum = 0;
            for (java.util.HashMap<String,IntWritable> val : values) {
                sum += val.get(file).get();
            }
            files.concat(" ").concat(file).concat("#").concat(Integer.toString(sum));
        }
        context.write(key, new Text(files));
        
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "invindex");

    for (int i = 0; i < args.length; i += 1) {
      if ("-skip".equals(args[i])) {
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
        i += 1;
        job.addCacheFile(new Path(args[i]).toUri());
      }
    }

    conf.set("mapreduce.map.output.compress", "true");
    conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec"); 
    conf.set("mapreduce.output.textoutputformat.separator", " | ");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setCombinerClass(Combine.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(10);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}