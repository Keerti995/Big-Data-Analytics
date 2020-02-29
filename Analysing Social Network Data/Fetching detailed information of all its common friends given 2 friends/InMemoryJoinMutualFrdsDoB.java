import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
/**
 * @author Keerti
 * Given any two Users (they are friend) as input, output the list of the names and the date of birth of their mutual friends.
 */
public class InMemoryJoinMutualFrdsDoB extends Configured implements Tool {
    static HashMap<String, String> hmap;

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text list = new Text();
        private Text userno = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration config = context.getConfiguration();
            hmap = new HashMap<String, String>();
            String userDataPath = config.get("userdata");
            FileSystem fs = FileSystem.get(config);
            Path path = new Path("hdfs://"+userDataPath);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line = br.readLine();
            while (line != null) {
                String[] arr = line.split(",");
                if (arr.length == 10) {
                    String data = arr[1] + ":" + arr[9];
                    hmap.put(arr[0].trim(), data);
                }
                line = br.readLine();
            }

            String[] user = value.toString().split("\t");
            if ((user.length == 2)) {

                String[] friend_list = user[1].split(",");
                int n = friend_list.length;
                StringBuilder res=new StringBuilder("[");
                for (int i = 0; i < friend_list.length; i++) {
                    if(hmap.containsKey(friend_list[i]))
                    {
                        if(i==(friend_list.length-1))
                            res.append(hmap.get(friend_list[i]));
                        else
                        {
                            res.append(hmap.get(friend_list[i]));
                            res.append(",");
                        }
                    }
                }
                res.append("]");
                userno.set(user[0]);
                list.set(res.toString());
                context.write(userno, list);
            }

        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new InMemoryJoinMutualFrdsDoB(), args);
        System.exit(res);

    }


    public int run(String[] otherArgs) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();
        // get all args
        if (otherArgs.length != 6) {
            System.err.println("Input : hadoop jar InMemoryJoinMutualFrdsDoB <userA> <userB> <soc-LiveJournalAdj1.txt> <temp_out> <userdata.txt> <o/p>");
            System.exit(2);
        }
        conf.set("userA", otherArgs[0]);
        conf.set("userB", otherArgs[1]);
        // create a job with name "InlineArgument" and assign all Jar,Mapper,Reducer,Output classes
        Job job = new Job(conf, "InlineArgument");
        job.setJarByClass(InMemoryJoinMutualFrdsDoB.class);
        job.setMapperClass(MutualFriendsGeneration.Map.class);
        job.setReducerClass(MutualFriendsGeneration.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        Path p=new Path(otherArgs[3]);
        FileOutputFormat.setOutputPath(job, p);
        int code = job.waitForCompletion(true) ? 0 : 1;

        Configuration conf1 = getConf();
        conf1.set("userdata", otherArgs[4]);
        Job job2 = new Job(conf1, "InMemoryJoinDateofBirth");
        job2.setJarByClass(InMemoryJoinMutualFrdsDoB.class);

        job2.setMapperClass(Map.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2,p);
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[5]));

        // Check job completion
        code = job2.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
        return code;

    }

}