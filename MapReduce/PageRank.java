package org.myorg;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.StringTokenizer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRank extends Configured implements Tool {
	
	public static final String OUTPUT = "output";
	public static Double pageCount=0.0;
	
	public static void main(String[] args) throws Exception, IOException, InterruptedException, ClassNotFoundException {
		int res  = ToolRunner .run( new PageRank(), args);
		  System .exit(res);
	}
	   
	public int run( String[] args) throws  Exception {
		Configuration parsingConf = new Configuration();//three different configurations for three different jobs
		Configuration pageRankConf = new Configuration();
		Configuration CleanupConf = new Configuration();
		int i = 0;
		String output = OUTPUT;

		// Job for parsing the file 
		Job parsingJob = new Job(parsingConf, "Parsing");
		parsingJob.setJarByClass(PageRank.class);
		
		parsingJob.setMapperClass(ParseMapper.class);
		parsingJob.setReducerClass(ParseReducer.class);
		
		parsingJob.setInputFormatClass(TextInputFormat.class);
		parsingJob.setOutputFormatClass(TextOutputFormat.class);
		
		parsingJob.setOutputKeyClass(Text.class);
		parsingJob.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(parsingJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(parsingJob, new Path(args[1]));
		
		parsingJob.waitForCompletion(true);
		

		// job for calculating pagerank iteratively
		while (i < 10) {
			
			Job pageRankJob = new Job(pageRankConf, "PageRank");
			pageRankJob.setJarByClass(PageRank.class);
			
			pageRankJob.setMapperClass(PageRankMapper.class);
			pageRankJob.setReducerClass(PageRankReducer.class);
			
			pageRankJob.setOutputKeyClass(Text.class);
			pageRankJob.setOutputValueClass(Text.class);
			
			pageRankJob.setInputFormatClass(TextInputFormat.class);
			pageRankJob.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.setInputPaths(pageRankJob, new Path(output + i));
			FileOutputFormat.setOutputPath(pageRankJob, new Path(output + (i + 1)));
			
			i++;
			pageRankJob.waitForCompletion(true);
		}

		// Job for cleanup and sorting

		Job CleanupJob = new Job(CleanupConf, "CleanAndSort");
		CleanupJob.setJarByClass(PageRank.class);
		
		CleanupJob.setMapperClass(CleanAndSortMapper.class);
		CleanupJob.setReducerClass(CleanAndSortReducer.class);
		CleanupJob.setNumReduceTasks(1);//to set the reducer to 1
		
		CleanupJob.setMapOutputValueClass(Text.class);
		CleanupJob.setMapOutputKeyClass(DoubleWritable.class);
		
		CleanupJob.setOutputKeyClass(Text.class);
		CleanupJob.setOutputValueClass(Text.class);
		
		CleanupJob.setInputFormatClass(TextInputFormat.class);
		CleanupJob.setOutputFormatClass(TextOutputFormat.class);
		
		CleanupJob.setSortComparatorClass(DoubleComparator.class);//Define the comparator that controls how the keys are sorted before they are passed to the Reducer.
		
		FileInputFormat.setInputPaths(CleanupJob, new Path(output + i));
		FileOutputFormat.setOutputPath(CleanupJob, new Path(output + (i + 1)));
		
		return CleanupJob.waitForCompletion(true) ? 0 : 1;


	}
	
	public static class ParseMapper extends Mapper<Object, Text, Text, Text> {//first map
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringBuilder stringBuilder = new StringBuilder();
			String lineText = value.toString();//line by line value
			String pageTitle = null;
			String text = null;
			
			if (!lineText.isEmpty()) {//if there is some text in the line
				//pageCount = pageCount+1;
				Pattern titleDataRegex = Pattern.compile("<title>(.*?)</title>");//gets the title patterns
				Matcher titleMatcher = titleDataRegex.matcher(lineText);//matches the pattern with the line
				
				while (titleMatcher.find()) {
					
					pageTitle = titleMatcher.group(1).trim();//it contains the title of the page
				}
				
				Pattern textRegex = Pattern.compile("<text(.*?)>(.*?)</text>");
				Matcher textMatcher = textRegex.matcher(lineText);
				
				while (textMatcher.find()) {
					
					text = textMatcher.group(2);//the text inside the tags <text> and </text>
				}
				if (text != null) {
					Pattern pattern = Pattern.compile("\\[\\[(.*?)\\]\\]");
					Matcher matcher = pattern.matcher(text);
					
					while (matcher.find()) {
						stringBuilder.append(matcher.group(1) + "###");//stringBuilder contains the text inside the links
					}
				}//try using LongSumReducer
				context.write(new Text(pageTitle), new Text(stringBuilder.toString()));//title||0.15\001 file1### file2###
			}
		}
	}
	
	public static class ParseReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
					
			StringBuilder sb = new StringBuilder();
			Double pageRank=0.00;
			try { 
				//if(pageCount != 0)
				//pageRank = 1.0/pageCount;
				for (Text value : values) {
					
					if (value.toString().contains("###") && value.toString().length() > 1) {//check for list of pages or links
						sb.append(value.toString());//append all the list of files
					}
				}
				context.write(new Text(key + "||" + "0.005" +"\001"), new Text(sb.toString().trim()));//writes the updated pagerank with list of values
			} catch (Exception e) {
				System.out.println(e);
			}
		}
	}

	public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text link = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {//title||0.15\001 file1### file2###

			if (value.toString().isEmpty() == false || value.toString() != null) {
				
				String[] titleRankAndFiles = value.toString().split("\001");//splits title+pagerank and list of files
				String[] titleAndRank = titleRankAndFiles[0].split("\\|\\|");//splits title and the page rank
				
				if (titleRankAndFiles[1].trim().length() > 0) {//if there exists links

					StringTokenizer stringTokenizer = new StringTokenizer(titleRankAndFiles[1], "###");//seperate the links with the delimiter
					//stringTokenizer contains all the outgoing links of a page
					int count = stringTokenizer.countTokens();//to get the count of the links
					
					while (stringTokenizer.hasMoreTokens()) {
						
						double pageRank = (Double.parseDouble(titleAndRank[1]) / new Double(count));//calculating new page rank based on links count
						link.set(stringTokenizer.nextToken().trim());//set the link with the links one by one
						
						if (pageRank != 0.0) {
							
							context.write(link, new Text(pageRank + ""));//link and its initial page rank
						}
					}
				}
				context.write(new Text(titleAndRank[0]), new Text(titleRankAndFiles[1]));//title and the list of files
			}
			
		}
	}

	public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
		private Text key2 = new Text();
		public static final double dampingFactor = 0.85;
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
					
			StringBuilder stringBuilder = new StringBuilder();
			double pageRank = 0.0;
			
			try {
				
				for (Text value : values) {
					
					if (value.toString().contains("###") && value.toString().length() > 1) {//check for list of pages or links
						stringBuilder.append(value.toString());//append all the list of files
						key2.set(key);
					} else {
						if (value.toString().length() > 1)
							pageRank += Double.parseDouble(value.toString().trim());//page rank of the pages getting added
					}
				}
				
				pageRank = (1 - dampingFactor) + (dampingFactor * pageRank);
				context.write(new Text(key2 + "||" + pageRank + "\001"), new Text(stringBuilder.toString().trim()));//writes the updated pagerank with list of values
				
			} catch (Exception e) {
				System.out.println(e);
			}
		}
	}
	
	public static class CleanAndSortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			try {
				
				String line = value.toString();//title||0.15\001 file1### file2###
				String[] titleRankAndFiles = line.split("\001");
				String[] titleAndRank = titleRankAndFiles[0].split("\\|\\|");
				DoubleWritable pageRank = new DoubleWritable((Double.parseDouble(titleAndRank[1])));
				
				if(titleRankAndFiles[1].contains("###") && titleRankAndFiles[1].length() > 1){
					
					context.write(pageRank, new Text(titleAndRank[0]));// pagrank is key and filename is value
					
				}
			} catch (Exception e) {
				
				System.out.println("Exception : " + e);
				
			}
		}
	}

	public static class CleanAndSortReducer extends Reducer<DoubleWritable, Text, Text, Text> {
		String output = OUTPUT;
		double totalPages = 0.0;
		int iterations = 10;

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			FileSystem fileSystem = FileSystem.get(context.getConfiguration());
			while (iterations >= 0) {
				fileSystem.delete(new Path(output + iterations), true);// deletes the intermediate output files
				iterations--;
			}
			super.cleanup(context);
		}
		protected void reduce(DoubleWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text text : values) {
				context.write(text, new Text(""+key));// reads only top 100 pagerank pages
			}
		}
	}

	public static class DoubleComparator extends WritableComparator {

		public DoubleComparator() {
			super(DoubleWritable.class);
		}
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			
			Double value1 = ByteBuffer.wrap(b1, s1, l1).getDouble();
			Double value2 = ByteBuffer.wrap(b2, s2, l2).getDouble();
			
			return value1.compareTo(value2) * (-1);
		}
	}
}
