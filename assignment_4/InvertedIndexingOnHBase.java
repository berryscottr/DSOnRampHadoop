package Assignment_4;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexingOnHBase {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{
        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            // Assign Indices to text
            String DocId = value.toString().substring(0, value.toString().indexOf("\t"));
            String value_raw =  value.toString().substring(value.toString().indexOf("\t") + 1);
            // Tokenize on every space
            StringTokenizer itr = new StringTokenizer(value_raw, " ");
            // Strip unwanted characters
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase());
                // Output all words and counts to reducer
                if(word.toString() != "" && !word.toString().isEmpty()){
                    context.write(word, new Text(DocId));
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            HashMap<String,Integer> map = new HashMap<String,Integer>();
            // Get count of every word to prepare for output
            for (Text val : values) {
                if (map.containsKey(val.toString())) {
                    map.put(val.toString(), map.get(val.toString()) + 1);
                } else {
                    map.put(val.toString(), 1);
                }
            }
            StringBuilder docValueList = new StringBuilder();
            for(String docID : map.keySet()){
                docValueList.append(docID + ":" + map.get(docID) + " ");
            }
            context.write(key, new Text(docValueList.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        // Create HBase Job
        HBaseConfiguration conf =  HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "InvertedIndexingOnHBase");
        job.setJarByClass(InvertedIndexingOnHBase.class);
        // Input from stackexchange HBase table
        TableMapReduceUtil.initTableMapperJob("stackexchange",
                TokenizerMapper.class, job);
        job.setMapperClass(TokenizerMapper.class);
        // Output to stackexchange Hbase Table
        TableMapReduceUtil.initTableReducerJob("stackexchange",
                IntSumReducer.class, job);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}