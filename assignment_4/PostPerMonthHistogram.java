package onramp.hbase;

import java.io.IOException;
import java.util.StringTokenizer;

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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

public class PostPerMonthHistogram extends Configured implements Tool {

    static final String META_CF = "POST_METADATA";
	static final String EDIT_CF = "EDIT";
	static final String BODY_CF = "BODY";
	static final String MONTH_CF = "MONTH";
	static final String POST_COUNT_COLUMN = "POST_COUNT";

	static IntWritable one = new IntWritable(1);

	static class PostPerMonthHistogramMapper extends TableMapper<IntWritable, IntWritable> {

		@Override
		protected void map(ImmutableBytesWritable rowKey, Result postData,
				Mapper<ImmutableBytesWritable, Result, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {

			String creationDateStr = Bytes
					.toString(postData.getValue(Bytes.toBytes(META_CF), Bytes.toBytes("CreationDate")));
			StringTokenizer creattionDateStrTokens = new StringTokenizer(creationDateStr, "-");
			creattionDateStrTokens.nextToken();// Discard the year
			IntWritable month = new IntWritable(Integer.valueOf(creattionDateStrTokens.nextToken()));
			context.write(month, one);
		}
	}

	static class PostPerMonthHistogramReducer<K> extends TableReducer<IntWritable, IntWritable, K> {

		@Override
		protected void reduce(IntWritable month, Iterable<IntWritable> intermedValues,
				Reducer<IntWritable, IntWritable, K, Mutation>.Context context)
				throws IOException, InterruptedException {

			int postCount = 0;
			for (IntWritable value : intermedValues) {
				postCount += value.get();
			}
			Put put = new Put(Bytes.toBytes(month.get()));
			put.addColumn(Bytes.toBytes(MONTH_CF), Bytes.toBytes(POST_COUNT_COLUMN), Bytes.toBytes(postCount));
			context.write(null, put);
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();

		Job job = Job.getInstance(conf, "PostPerMonthHistogram");

		job.setJarByClass(PostPerMonthHistogram.class);

		job.setMapperClass(PostPerMonthHistogramMapper.class);
		job.setReducerClass(PostPerMonthHistogramReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TableInputFormat.class);
		job.getConfiguration().set(TableInputFormat.INPUT_TABLE, "stackexchange");

		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "post_month_histogram");
		TableMapReduceUtil.addDependencyJars(job);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int runExitCode = ToolRunner.run(new PostPerMonthHistogram(), args);
		System.exit(runExitCode);
	}
}

