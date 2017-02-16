package com.hari.IndexApp;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hari.IndexApp.App;

public final class App extends Configured implements Tool {

	  
	  public static void main(final String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new App(), args);
	    System.exit(res);
	  }

	 
	  public int run(final String[] args) throws Exception {

	    if (args.length != 2) {
		            System.out.println("usage: [input] [output]");
		            System.exit(-1);
		}

	    Path input = new Path(args[0]);
	    Path output = new Path(args[1]);

	    Configuration   conf  =   getConf();

		Job job = Job.getInstance(conf, this.getClass().toString());
		
	    job.setJarByClass(App.class);
	    job.setMapperClass(IndexMapper.class);
	    job.setReducerClass(IndexReducer.class);

	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);

	    FileInputFormat.setInputPaths(job, input);
	    FileOutputFormat.setOutputPath(job, output);

	    return job.waitForCompletion(true) ? 0 : 1;
	  }

	  
	  // for each word, emit <word, file containing the word>
	  
	  // recall that setup() and cleanup() are executed only once
	  // for each input split
	  
	  public static class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {

	    private Text documentId;
	    private Text word = new Text();
	    HashMap<String , String> h = new HashMap<String, String>();
	    @Override
	    protected void setup(Context context) {
	      String filename =
	          ((FileSplit) context.getInputSplit()).getPath().getName();
	      documentId = new Text(filename);
	    }

	    @Override
	    protected void map(LongWritable key, Text value, Context context)
	        throws IOException, InterruptedException {    	
	    	String newStr= value.toString().replaceAll("[0-9.!?]"," ");
	    	//String newStr= value.toString();
	      for (String token : StringUtils.split(newStr)) {    	    
	    	  word.set(token);    
	          h.put(token, documentId.toString());
	          //context.write(word, documentId);
	      }
	      
	    }
	    @Override
	    protected void cleanup(Context context)throws IOException, InterruptedException{
	    	for(String key: h.keySet()){  		
	         context.write(new Text(key),new Text(h.get(key)));   
	        }
	    	
	    }
	  }

	  // combine all the filenames containing the same word
	  
	  public static class IndexReducer extends Reducer<Text, Text, Text, Text> {

	    private Text docIds = new Text();

	    @Override
	    public void reduce(Text key, Iterable<Text> values, Context context)
	        throws IOException, InterruptedException {

	      HashSet<String> uniqueDocIds = new HashSet<String>();
	      for (Text docId : values) {
	        uniqueDocIds.add(docId.toString());
	      }
	      docIds.set(new Text(StringUtils.join(uniqueDocIds, ",")));
	      context.write(key, docIds);
	    }
	  }
	}