import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Keerti
 * Find friend pairs whose common friend number are within the top-10 in all the pairs
 */
public class UserTopTenFriendsQ2 {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        final Logger log = LoggerFactory.getLogger(Map.class);
        private Text coupleKey = new Text();

        /**
         * divides the user and its direct friends and passes it as the i/p for reduce fn
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] userinfo = value.toString().split("\t");
            if (userinfo.length == 2) {
                String[] friendList = userinfo[1].split(",");
                for (String data : friendList) {
                    String friendPairKey = "";
                    if (Integer.parseInt(userinfo[0]) > Integer.parseInt(data))
                        friendPairKey = data + ", " + userinfo[0];
                    else
                        friendPairKey = userinfo[0] + ", " + data;

                    coupleKey.set(friendPairKey);
                    context.write(coupleKey, new Text(userinfo[1]));
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        /**
         * retains all the common friends
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> friendsList1 = new HashSet<String>();
            HashSet<String> friendsList2 = new HashSet<String>();

            int index = 1;
            for (Text val : values) {
                if (index == 1) {
                    friendsList1.addAll(Arrays.asList(val.toString().split(",")));

                } else if (index == 2) {
                    friendsList2.addAll(Arrays.asList(val.toString().split(",")));
                }
                index++;
            }

            friendsList1.retainAll(friendsList2);

            StringBuilder sb = new StringBuilder();
            for (String s : friendsList1) {
                sb.append(s).append(",");
            }
            if (sb.length() > 0) {
                result.set(sb.substring(0, sb.length() - 1).toString());
                context.write(key, result);
            }
        }
    }

    public static class Map2 extends Mapper<Text, Text, LongWritable, Text> {
        private LongWritable frequency = new LongWritable();

        /**
         *maps frequency to the value
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int count = value.toString().split(",").length;
            frequency.set(count);
            context.write(frequency, new Text(key.toString() + "-" + value));
        }
    }

    public static class Reduce2 extends Reducer<LongWritable, Text, Text, Text> {

        private int topTenCount = 0;

        /**
         * counts the top ten values of the friend list
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Text pairValue = new Text();
            for (Text value : values) {
                if (topTenCount < 10) {
                    topTenCount++;
                    String[] temp = value.toString().split("-");
                    pairValue.set(key.toString() + "\t" + temp[1]);
                    context.write(new Text(temp[0]), pairValue);
                }
            }
        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 3) {
            System.err.println("Input Arguments Check");
            System.exit(2);
        }
        // create a job with name "UserTopTenFriendsQ2" and assign all Jar,Mapper,Reducer,Output classes
        // first job - gives pairs and their mutual friends list
        Job getMutualFriendsJob = Job.getInstance(conf1, "UserTopTenFriendsQ2");
        getMutualFriendsJob.setJarByClass(UserTopTenFriendsQ2.class);
        getMutualFriendsJob.setMapperClass(UserTopTenFriendsQ2.Map.class);
        getMutualFriendsJob.setReducerClass(UserTopTenFriendsQ2.Reduce.class);
        getMutualFriendsJob.setOutputKeyClass(Text.class);
        getMutualFriendsJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(getMutualFriendsJob, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(getMutualFriendsJob, new Path(otherArgs[1]));
        boolean firstMrJobStatus = getMutualFriendsJob.waitForCompletion(true);
        if (!firstMrJobStatus) {
            System.exit(1);
        } else
        // Second Job - gives pairs, count of mutual friends , and mutual friends list
        {
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "MyTopTenFriends");
            job2.setJarByClass(UserTopTenFriendsQ2.class);
            job2.setMapperClass(UserTopTenFriendsQ2.Map2.class);
            job2.setReducerClass(UserTopTenFriendsQ2.Reduce2.class);
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setInputFormatClass(KeyValueTextInputFormat.class);
            job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
            FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }

    }
}


