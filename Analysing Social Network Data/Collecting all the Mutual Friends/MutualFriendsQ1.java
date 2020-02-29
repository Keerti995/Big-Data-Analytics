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
import java.util.LinkedHashSet;

/**
 * @author Keerti
 * Gets all the mutual friends of the users - soc-LiveJournalAdj1.txt
 */
public class MutualFriendsQ1 {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        Text userno = new Text();
        Text friendslist = new Text();

        /**
         * split the line into user and friends
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\\t");
            String userid = line[0];
            if( line.length == 1 ) {
                return;
            }
            String[] others = line[1].split(",");
            for( String friend : others ) {

                if( userid.equals(friend) )
                    continue;

                String userKey = (Integer.parseInt(userid) < Integer.parseInt(friend) ) ? userid + "," + friend : friend + "," + userid;
                String regex = "((\\b" + friend + "[^\\w]+)|\\b,?" + friend + "$)";
                friendslist.set(line[1].replaceAll(regex, ""));
                userno.set(userKey);
                context.write(userno, friendslist);
            }
        }

    }

    public static class Reduce extends Reducer<Text,Text,Text,Text> {
        /**
         * Creates hashset of 2 friendslist and retains all the common friends
         * @param friendslist1
         * @param friendslist2
         * @return
         */
        private String findMatchingFriends( String friendslist1, String friendslist2 ) {

            if( friendslist1 == null || friendslist2 == null )
                return null;

            String[] friendsList1 = friendslist1.split(",");
            String[] friendsList2 = friendslist2.split(",");

            //use LinkedHashSet to retain the sort order
            LinkedHashSet<String> hashset1 = new LinkedHashSet<String>();
            for( String user: friendsList1 ) {
                hashset1.add(user);
            }

            LinkedHashSet<String> hashset2 = new LinkedHashSet<String>();
            for( String user: friendsList2 ) {
                hashset2.add(user);
            }

            //retian all common friends between both the friendslists
            hashset1.retainAll(hashset2);

            return hashset1.toString().replaceAll("\\[|\\]","");
        }

        /**
         * Arranges in the expected format and sends the output
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {

            String[] friendsList = new String[2];
            int i = 0;

            for( Text value: values ) {
                friendsList[i++] = value.toString();
            }
            String mutualFriends = findMatchingFriends(friendsList[0],friendsList[1]);
            if( mutualFriends != null && mutualFriends.length() != 0 ) {
                context.write(key, new Text( mutualFriends ) );
            }
        }

    }

    /**
     *Driver program : set all configurations and classes to the job
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Input: hadoop jar MutualFriendsQ1.jar <soc-LiveJournalAdj1.txt> <output>");
            System.exit(2);
        }

        // create a job with name "MutualFriendsQ1" and assign all Jar,Mapper,Reducer,Output classes
        Job job = new Job(conf, "MutualFriendsQ1");
        job.setJarByClass(MutualFriendsQ1.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //check job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}