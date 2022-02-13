package onramp.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConnectedComponents extends Configured implements Tool {

    public static final String NEIGHBOR_COMPID = "*";

	public static enum UpdateCounter {
		UPDATED;
	}

	public static class ConnectedComponentsMapper extends Mapper<Text, Text, Text, Text> {

		private String ajListDelim, compIdDelim, ajListStr, compIdStr, nodeIdStr;
		private Configuration conf;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			ajListDelim = conf.get("ajListDelim");
			compIdDelim = conf.get("compIdDelim");
		}

		public void map(Text nodeId, Text value, Context context) throws IOException, InterruptedException {

			String valueStr = value.toString();

			nodeIdStr = nodeId.toString();
			ArrayList<String> tokens = new ArrayList<String>(Arrays.asList(valueStr.split(compIdDelim)));
			boolean firstIteration = false;
			if (tokens.size() == 1) {
				ajListStr = tokens.get(0);
				firstIteration = true;
			} else {
				compIdStr = tokens.get(0);
				ajListStr = tokens.get(1);
			}

			ArrayList<String> ajList = new ArrayList<String>(Arrays.asList(ajListStr.split(ajListDelim)));
			if (firstIteration) {
				String minNeighborId = Collections.min(ajList);
				if (nodeIdStr.compareTo(minNeighborId) < 0) {
					compIdStr = nodeIdStr;
				} else {
					compIdStr = minNeighborId;
				}
			}

			for (String ajNode : ajList) {

				context.write(new Text(ajNode), new Text(NEIGHBOR_COMPID + compIdStr));
			}

			context.write(nodeId, new Text(compIdStr + compIdDelim + ajListStr));
		}
	}

	public static class ConnectedComponentsReducer extends Reducer<Text, Text, Text, Text> {

		private String compIdDelim, compIdStr, ajListStr;
		private Configuration conf;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			compIdDelim = conf.get("compIdDelim");
		}

		public void reduce(Text nodeId, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String valueStr = null;
			ArrayList<String> neighborCompIds = new ArrayList<String>();

			for (Text value : values) {
				valueStr = value.toString();
				if (valueStr.startsWith(NEIGHBOR_COMPID)) {
					neighborCompIds.add(valueStr.substring(NEIGHBOR_COMPID.length()));
				} else {
					ArrayList<String> tokens = new ArrayList<String>(Arrays.asList(valueStr.split(compIdDelim)));
					compIdStr = tokens.get(0);
					ajListStr = tokens.get(1);
				}
			}

			String minNeighborCompId = Collections.min(neighborCompIds);
			if (minNeighborCompId.compareTo(compIdStr) < 0) {
				context.getCounter(UpdateCounter.UPDATED).increment(1);
				compIdStr = minNeighborCompId;
			}
			context.write(nodeId, new Text(compIdStr + compIdDelim + ajListStr));
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		String ajListDelim = args[2];
		String compIdDelim = args[3];

		Configuration conf;
		Job job;
		boolean runExitCode = false;
		Path iteration_out_path = in;
		int iteration = 0;
		boolean updated = true;
		do {

			iteration++;

			conf = getConf();
			in = iteration_out_path;
			iteration_out_path = new Path(out, "cc-" + Integer.toString(iteration));
			conf.set("ajListDelim", ajListDelim);
			conf.set("compIdDelim", compIdDelim);

			job = Job.getInstance(conf, "ConnectedComponents_Iteration_" + Integer.toString(iteration));
			
			job.setJarByClass(ConnectedComponents.class);

			FileInputFormat.setInputPaths(job, in);
			FileOutputFormat.setOutputPath(job, iteration_out_path);

			job.setMapperClass(ConnectedComponentsMapper.class);
			job.setReducerClass(ConnectedComponentsReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setInputFormatClass(KeyValueTextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			runExitCode = job.waitForCompletion(true);
			if (job.getCounters().findCounter(UpdateCounter.UPDATED).getValue() == 0) {
				System.out.println("Nothing updated!!");
				updated = false;
			}
		} while (updated);

		return runExitCode ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int runExitCode = ToolRunner.run(new ConnectedComponents(), args);
		System.exit(runExitCode);
	}
}

