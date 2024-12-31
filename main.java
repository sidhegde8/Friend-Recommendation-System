import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class question_1 {

    public static class FriendMapper extends Mapper<LongWritable, Text, IntWritable, Text> 
    {
        private IntWritable user = new IntWritable();
        private Text val = new Text();

        @Override
        protected void map(LongWritable key_value, Text val, Context c) throws IOException, InterruptedException {
            String[] toke = val.toString().split("\t");

            if (toke.length == 2) 
            {
                int userID = Integer.parseInt(toke[0].trim());
                String[] connections = toke[1].trim().split(",");

                user.set(userID);
                val.set("FRIENDS_LIST:" + toke[1]);
                c.write(user, val);

                for (String connection : connections) {
                    int f_ID = Integer.parseInt(connection.trim());
                    user.set(f_ID);
                    val.set("FRIEND_OF:" + userID + ":" + toke[1]);
                    c.write(user, val);
                }
            }
        }
    }

    public static class FriendReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    
    @Override
    protected void reduce(IntWritable key_value, Iterable<Text> vals, Context c) throws IOException, InterruptedException {
        int userID = key_value.get();
        Set<Integer> directFriends = new HashSet<>();
        Map<Integer, Integer> potentialFriends = new HashMap<>();
        List<String> connectionsOfFriendsData = new ArrayList<>();

        for (Text v : vals) 
        {
            String val = v.toString();
            if (val.startsWith("FRIENDS_LIST:")) 
            {
                String[] connections = val.substring("FRIENDS_LIST:".length()).split(",");
                for (String connection : connections) 
                {
                    directFriends.add(Integer.parseInt(connection.trim()));
                }
            } 
            else if (val.startsWith("FRIEND_OF:")) 
            {
                connectionsOfFriendsData.add(val.substring("FRIEND_OF:".length()));
            }
        }

        List<int[]> connection_of_f = new ArrayList<>();
        for (String data : connectionsOfFriendsData) 
        {
            String[] toke = data.split(":", 2);
            int f_ID = Integer.parseInt(toke[0].trim());
            String[] connections = toke[1].split(",");
            for (String connectionOfFriend : connections) 
            {
                int friend_ID = Integer.parseInt(connectionOfFriend.trim());
                if (friend_ID != userID && !directFriends.contains(friend_ID)) 
                {
                    connection_of_f.add(new int[]{friend_ID, f_ID}); // Store potential friends with the originating friend
                }
            }
        }

        for (int i = 0; i < connection_of_f.size(); i++) 
        {
            int[] fofPair = connection_of_f.get(i);
            int pote_Friend = fofPair[0];
            int origin = fofPair[1];
            
            int mutual = 1;
            for (int j = i + 1; j < connection_of_f.size(); j++) {
                int[] other = connection_of_f.get(j);
                if (other[0] == pote_Friend && other[1] != origin) 
                {
                    mutual++;
                    connection_of_f.remove(j); 
                    j--; 
                }
            }

            potentialFriends.put(pote_Friend, potentialFriends.getOrDefault(pote_Friend, 0) + mutual);
        }

        List<Map.Entry<Integer, Integer>> recs = new ArrayList<>(potentialFriends.entrySet());
        recs.sort((a, b) -> 
        {
            int compare = b.getValue().compareTo(a.getValue()); 
            if (compare != 0) {
                return compare;
            }
            return a.getKey().compareTo(b.getKey()); 
        });

        
        StringBuilder recList = new StringBuilder();
        int count = 0;
        for (Map.Entry<Integer, Integer> entry : recs) 
        {
            if (count >= 10) {
                break;
            }
            if (count > 0) {
                recList.append(",");
            }
            recList.append(entry.getKey());
            count++;
        }

        
        c.write(key_value, new Text(recList.toString()));
    }
}


    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: question_1 <input_path> <output_path>");
            System.exit(-1);
        }

        Configuration con = new Configuration();
        Job job = Job.getInstance(con, "People You Might Know");

        job.setJarByClass(question_1.class);
        job.setMapperClass(FriendMapper.class);
        job.setReducerClass(FriendReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
