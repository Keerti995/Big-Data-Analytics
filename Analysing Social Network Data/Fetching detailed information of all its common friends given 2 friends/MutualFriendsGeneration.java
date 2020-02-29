import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.HashSet;

/**
 * @author Keerti
 * Generates Mutual Friends of 2 friends : Used in InMemoryJoinMutualFrdsDoB
 */

public class MutualFriendsGeneration {
    static String user1 = "";
    static String user2 = "";
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text userno = new Text();
        private Text infolist = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration config = context.getConfiguration();
            user1=config.get("userA");
            user2 = config.get("userB");
            String[] userinfo = value.toString().split("\t");
            if ((userinfo.length == 2)&&(userinfo[0].equals(user1)||userinfo[0].equals(user2))) {

                String[] friend_list = userinfo[1].split(",");
                infolist.set(userinfo[1]);
                for (int i = 0; i < friend_list.length; i++) {
                    String map_key;
                    if (Integer.parseInt(userinfo[0]) < Integer.parseInt(friend_list[i])) {
                        map_key = userinfo[0] + "," + friend_list[i];
                    } else {
                        map_key = friend_list[i] + "," + userinfo[0];
                    }
                    userno.set(map_key);
                    context.write(userno, infolist);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet hashSet = new HashSet();
            int i = 0;

            String temp ="";
            for (Text value : values) {
                String[] val = value.toString().split(",");
                for (int j = 0; j < val.length; j++) {
                    if (i == 0) {
                        hashSet.add(val[j]);
                    } else {
                        if (hashSet.contains(val[j])) {
                            temp=temp.concat(val[j]);
                            temp=temp.concat(",");
                            hashSet.remove(val[j]);
                        }

                    }
                }
                i++;
            }
            if(!temp.equals("")){
                result.set(temp);
                context.write(key, result);
            }
        }

    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 4) {
            System.err.println("Input: hadoop jar MutualFriendsGeneration.jar <userA> <userB> <in> <out>");
            System.exit(2);
        }
        conf.set("userA", otherArgs[0]);
        conf.set("userB", otherArgs[1]);
        // create a job with name "MutualFriendsGeneration" and assign all Jar,Mapper,Reducer,Output classes
        Job job = new Job(conf, "MutualFriendsGeneration");
        job.setJarByClass(MutualFriendsGeneration.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
        // Checking job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}