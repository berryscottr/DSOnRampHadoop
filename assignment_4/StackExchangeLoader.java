package onramp.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

public class StackExchangeLoader extends Configured implements Tool {

    static String POST_METADATA_CF = "POST_METADATA";
	static String OWNER_CF = "OWNER";
	static String EDIT_CF = "EDIT";
	static String BODY_CF = "BODY";

	static class StackExchangeLoaderMapper<K> extends Mapper<LongWritable, Text, K, Put> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			if (key.equals(new LongWritable(0))){
				return;
			}
			context.write(null, StackExchangeLineParser.parse(value.toString()));
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();

		Job job = Job.getInstance(conf, "StackExchangeLoader");

		job.setJarByClass(StackExchangeLoader.class);

		job.setMapperClass(StackExchangeLoaderMapper.class);

		job.setNumReduceTasks(0);

		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));

		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "stackexchange");
		TableMapReduceUtil.addDependencyJars(job);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		int runExitCode = ToolRunner.run(new StackExchangeLoader(), args);
		System.exit(runExitCode);
	}

	private static class StackExchangeLineParser {

		static int Id = 0;//
		static int Body = 1;//
		static int Tags = 2;//
		static int Title = 3;//
		static int Score = 4;//
		static int ParentId = 5;//
		static int ViewCount = 6;//
		static int ClosedDate = 7;//
		static int PostTypeId = 8;//
		static int AnswerCount = 9;
		static int OwnerUserId = 10;//
		static int LastEditDate = 11;//
		static int CommentCount = 12;//
		static int CreationDate = 13;//
		static int FavoriteCount = 14;//
		static int LastActivityDate = 15;//
		static int LastEditorUserId = 16;//
		static int AcceptedAnswerId = 17;//
		static int OwnerDisplayName = 18;//
		static int CommunityOwnedDate = 19;//
		static int LastEditorDisplayName = 20;//

		public static Put parse(String line) {

			StringTokenizer tokens = new StringTokenizer(line.toString(), "\t");
			ArrayList<String> vals = new ArrayList<String>();
			while (tokens.hasMoreTokens()) {
				vals.add(tokens.nextToken());
			}

			Put put = new Put(Bytes.toBytes(vals.get(Id)));
			byte[] columnFamily, qualifier;

			try {
				/*******************************************************************************/
				columnFamily = Bytes.toBytes(POST_METADATA_CF);
				qualifier = Bytes.toBytes("ParentId");
				put.addColumn(columnFamily, qualifier, Bytes.toBytes(vals.get(ParentId)));
				qualifier = Bytes.toBytes("PostTypeId");
				put.addColumn(columnFamily, qualifier, Bytes.toBytes(vals.get(PostTypeId)));
				qualifier = Bytes.toBytes("Tags");
				put.addColumn(columnFamily, qualifier, Bytes.toBytes(vals.get(Tags)));
				qualifier = Bytes.toBytes("ClosedDate");
				put.addColumn(columnFamily, qualifier, Bytes.toBytes(vals.get(ClosedDate)));
				qualifier = Bytes.toBytes("CreationDate");
				put.addColumn(columnFamily, qualifier, Bytes.toBytes(vals.get(CreationDate)));
				qualifier = Bytes.toBytes("LastActivityDate");
				put.addColumn(columnFamily, qualifier, Bytes.toBytes(vals.get(LastActivityDate)));
				qualifier = Bytes.toBytes("CommunityOwnedDate");
				put.addColumn(columnFamily, qualifier, Bytes.toBytes(vals.get(CommunityOwnedDate)));
				qualifier = Bytes.toBytes("AcceptedAnswerId");
				put.addColumn(columnFamily, qualifier, Bytes.toBytes(vals.get(AcceptedAnswerId)));
				qualifier = Bytes.toBytes("FavoriteCount");
				put.addColumn(columnFamily, qualifier, Bytes.toBytes(vals.get(FavoriteCount)));
				qualifier = Bytes.toBytes("ViewCount");
				put.addColumn(columnFamily, qualifier, Bytes.toBytes(vals.get(ViewCount)));
				qualifier = Bytes.toBytes("CommentCount");
				put.addColumn(columnFamily, qualifier, Bytes.toBytes(vals.get(CommentCount)));
				qualifier = Bytes.toBytes("AnswerCount");
				put.addColumn(columnFamily, qualifier, Bytes.toBytes(vals.get(AnswerCount)));
				qualifier = Bytes.toBytes("Score");
				put.addColumn(columnFamily, qualifier, Bytes.toBytes(vals.get(Score)));

				/*******************************************************************************/
				columnFamily = Bytes.toBytes(OWNER_CF);
				qualifier = Bytes.toBytes("OwnerUserId");
				put.addColumn(columnFamily, qualifier, Bytes.toBytes(vals.get(OwnerUserId)));
				qualifier = Bytes.toBytes("OwnerDisplayName");
				put.addColumn(columnFamily, qualifier, Bytes.toBytes(vals.get(OwnerDisplayName)));

				/*******************************************************************************/
				columnFamily = Bytes.toBytes(EDIT_CF);
				qualifier = Bytes.toBytes("LastEditorDisplayName");
				put.addColumn(columnFamily, qualifier, Bytes.toBytes(vals.get(LastEditorDisplayName)));
				qualifier = Bytes.toBytes("LastEditorUserId");
				put.addColumn(columnFamily, qualifier, Bytes.toBytes(vals.get(LastEditorUserId)));
				qualifier = Bytes.toBytes("LastEditDate");
				put.addColumn(columnFamily, qualifier, Bytes.toBytes(vals.get(LastEditDate)));

				/*******************************************************************************/
				columnFamily = Bytes.toBytes(BODY_CF);
				qualifier = Bytes.toBytes("Title");
				put.addColumn(columnFamily, qualifier, Bytes.toBytes(vals.get(Title)));
				qualifier = Bytes.toBytes("Body");
				put.addColumn(columnFamily, qualifier, Bytes.toBytes(vals.get(Body)));

			} catch (Exception e) {
				e.printStackTrace();
			}
			return put;
		}
	}

}

