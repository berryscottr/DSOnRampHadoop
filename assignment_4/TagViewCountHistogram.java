package onramp.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

public class TagViewCountHistogram extends Configured implements Tool {

    static final String META_CF = "POST_METADATA";
	static final String STATS_CF = "STATS";
	static final String VIEWCOUNT_QUALIFIER = "ViewCount";
	static final String TAGS_QUALIFIER = "Tags";

	static IntWritable one = new IntWritable(1);

	static class TagViewCountHistogramMapper extends TableMapper<Text, IntWritable> {

		@Override
		protected void map(ImmutableBytesWritable rowKey, Result postData,
				Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			String allTags=null;
			allTags = Bytes.toString(postData.getValue(Bytes.toBytes(META_CF), Bytes.toBytes(TAGS_QUALIFIER)));
			
			if (allTags != null && !allTags.isEmpty() && allTags.length() > 1) {
				ArrayList<String> tags = new ArrayList<String>(
						Arrays.asList(allTags.substring(1, allTags.length() - 1).split("><")));
				IntWritable viewCount = new IntWritable(
						Integer.valueOf(Bytes.toString(postData.getValue(Bytes.toBytes(META_CF), Bytes.toBytes(VIEWCOUNT_QUALIFIER)))));
				for (String tag : tags) {
					context.write(new Text(tag), viewCount);
				}
			}
		}
	}

	static class TagViewCountHistogramReducer<K> extends TableReducer<Text, IntWritable, K> {

		@Override
		protected void reduce(Text tag, Iterable<IntWritable> intermedValues,
				Reducer<Text, IntWritable, K, Mutation>.Context context) throws IOException, InterruptedException {

			int viewCunt = 0;
			for (IntWritable value : intermedValues) {
				viewCunt += value.get();
			}
			Put put = new Put(Bytes.toBytes(tag.toString()));
			put.addColumn(Bytes.toBytes(STATS_CF), Bytes.toBytes(VIEWCOUNT_QUALIFIER), Bytes.toBytes(viewCunt));
			context.write(null, put);
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();

		Job job = Job.getInstance(conf, "TagViewCountHistogram");

		job.setJarByClass(TagViewCountHistogram.class);

		job.setMapperClass(TagViewCountHistogramMapper.class);
		job.setReducerClass(TagViewCountHistogramReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TableInputFormat.class);
		job.getConfiguration().set(TableInputFormat.INPUT_TABLE, "stackexchange");

		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "tag_viewcount_histogram");
		TableMapReduceUtil.addDependencyJars(job);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int runExitCode = ToolRunner.run(new TagViewCountHistogram(), args);
		System.exit(runExitCode);
	}
}

