import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai3 {

    public static class GenderMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, String> userGenders = new HashMap<>();
        private Map<String, String> movieTitles = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length >= 2) {
                BufferedReader movieReader = new BufferedReader(new FileReader("movies.txt"));
                String line;
                while ((line = movieReader.readLine()) != null) {
                    String[] tokens = line.split(", ");
                    if (tokens.length >= 2) {
                        String movieID = tokens[0].trim();
                        String title = tokens[1].trim();
                        movieTitles.put(movieID, title);
                    }
                }
                movieReader.close();

                BufferedReader userReader = new BufferedReader(new FileReader("users.txt"));
                while ((line = userReader.readLine()) != null) {
                    String[] tokens = line.split(", ");
                    if (tokens.length >= 2) {
                        String userID = tokens[0].trim();
                        String gender = tokens[1].trim();
                        userGenders.put(userID, gender);
                    }
                }
                userReader.close();
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(", ");
            if (tokens.length >= 3) {
                String userID = tokens[0].trim();
                String movieID = tokens[1].trim();
                float rating = Float.parseFloat(tokens[2].trim());
                
                String gender = userGenders.get(userID);
                String movieTitle = movieTitles.get(movieID);
                
                if (gender != null && movieTitle != null) {
                    context.write(new Text(movieTitle), new Text(gender + ":" + rating));
                }
            }
        }
    }

    public static class GenderReducer extends Reducer<Text, Text, Text, Text> {
        
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            float maleSum = 0, femaleSum = 0;
            int maleCount = 0, femaleCount = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                if (parts.length == 2) {
                    String gender = parts[0];
                    float rating = Float.parseFloat(parts[1]);
                    
                    if (gender.equals("M")) {
                        maleSum += rating;
                        maleCount++;
                    } else if (gender.equals("F")) {
                        femaleSum += rating;
                        femaleCount++;
                    }
                }
            }

            float maleAvg = maleCount > 0 ? maleSum / maleCount : 0;
            float femaleAvg = femaleCount > 0 ? femaleSum / femaleCount : 0;
            
            String result = String.format("Male: %.2f, Female: %.2f", maleAvg, femaleAvg);
            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: Bai3 <movies_path> <users_path> <ratings_1_path> <ratings_2_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gender Rating Analysis");
        job.setJarByClass(Bai3.class);

        job.setMapperClass(GenderMapper.class);
        job.setReducerClass(GenderReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new Path(args[0]).toUri());
        job.addCacheFile(new Path(args[1]).toUri());

        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileInputFormat.addInputPath(job, new Path(args[3]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
