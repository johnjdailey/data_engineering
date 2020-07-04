// adapted from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;
import org.apache.hadoop.io.DoubleWritable;

public class RedditAverage extends Configured implements Tool {

	public static class RedditMapper
	extends Mapper<LongWritable, Text, Text, LongPairWritable>{

		private final static Long one = new Long(1);
		private Text subreddit = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {

			JSONObject record = new JSONObject(value.toString());

			subreddit.set((String)record.get("subreddit"));
			int score = (int)record.get("score");

			LongPairWritable pair = new LongPairWritable();
			pair.set(one, score);

			context.write(subreddit,pair);

		}
	}

	public static class RedditCombiner
	extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
		private LongPairWritable result = new LongPairWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int total_comments = 0;
			int total_score = 0;
			for (LongPairWritable val : values) {
				total_comments += val.get_0();
				total_score += val.get_1();
			}
			result.set(total_comments, total_score);
			context.write(key, result);
		}
	}

	public static class RedditReducer
	extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
			double total_comments = 0;
			int total_score = 0;
			for (LongPairWritable val : values) {
				total_comments += val.get_0();
				total_score += val.get_1();
			}
			result.set(total_score/total_comments);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "Reddit Average");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(RedditMapper.class);
		job.setCombinerClass(RedditCombiner.class);
		job.setReducerClass(RedditReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongPairWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
