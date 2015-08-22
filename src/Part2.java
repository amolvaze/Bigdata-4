/********************************************************************
 * Program by:- Amol Vaze  & Net id:- asv130130@utdallas.edu
 Program lists user_id, sex and age of users who rated the movie 4 or greater
 ********************************************************************/
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Part2 {

	public static HashMap<String, String> HashMap;
	private static Text text_rating;

	// Map code goes here
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		String mymovieid;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			super.setup(context);

			Configuration conf = context.getConfiguration();
			HashMap = new HashMap<String, String>();
			mymovieid = conf.get("movieid"); // for retrieving data you set in
												// driver code
			@SuppressWarnings("deprecation")
			Path[] localPaths = context.getLocalCacheFiles();
			for (Path myfile : localPaths) {
				String line = null;
				String nameofFile = myfile.getName();
				File file = new File(nameofFile + "");
				FileReader fr = new FileReader(file);
				BufferedReader br = new BufferedReader(fr);
				line = br.readLine();
				while (line != null) {
					String[] arr = line.split("::");
					HashMap.put(arr[0].trim(), arr[0] + arr[1] + arr[2]); // userid
																			// and
																			// gender
																			// and
																			// age
					line = br.readLine();
				}

				// closing connection
				br.close();
			}
		}

		// Reading rating.dat file in this method
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String str = value.toString();
			// UserID::moviesID::Ratings::Timestamp
			String[] array1 = str.split("::");
			Text user_id = new Text(array1[0]);
			@SuppressWarnings("unused")
			Text movie_id = new Text(array1[1]);
			int int_rating = Integer.parseInt(array1[2]);
			String intrating = array1[2];

			if (int_rating >= 4 && mymovieid.equals(array1[1])) {
				text_rating = new Text("rat~" + intrating);
				user_id.set(HashMap.get(array1[0]));
				context.write(user_id, text_rating);
			}

		}

	}

	// Driver code
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		// get all args
		if (otherArgs.length != 3) {
			System.err
					.println("Usage: JoinExample <in> <in2> <out> <anymovieid>");
			System.exit(2);
		}

		conf.set("movieid", otherArgs[2]); // setting global data variable for
											// hadoop

		// create a job with name "joinexc"
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "joinexc");
		job.setJarByClass(Part2.class);

		final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
		job.addCacheFile(new URI(NAME_NODE + "/user/hue/users.dat"));
		job.setJarByClass(Part2.class);

		// OPTIONAL :: uncomment the following line to add the Combiner
		// job.setCombinerClass(Reduce.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
				TextInputFormat.class, Map1.class);

		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);

		// set the HDFS path of the input data
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.waitForCompletion(true);

	}

}
