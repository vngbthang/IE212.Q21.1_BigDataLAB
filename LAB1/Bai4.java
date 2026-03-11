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

public class Bai4 {

    public static class AgeGroupMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, Integer> userAges = new HashMap<>();
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
                    if (tokens.length >= 3) {
                        String userID = tokens[0].trim();
                        int age = Integer.parseInt(tokens[2].trim());
                        userAges.put(userID, age);
                    }
                }
                userReader.close();
            }
        }

        private String getAgeGroup(int age) {
            if (age <= 18) return "0-18";
            else if (age <= 35) return "18-35";
            else if (age <= 50) return "35-50";
            else return "50+";
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(", ");
            if (tokens.length >= 3) {
                String userID = tokens[0].trim();
                String movieID = tokens[1].trim();
                float rating = Float.parseFloat(tokens[2].trim());
                
                Integer age = userAges.get(userID);
                String movieTitle = movieTitles.get(movieID);
                
                if (age != null && movieTitle != null) {
                    String ageGroup = getAgeGroup(age);
                    // Emit: (MovieTitle, "AgeGroup:Rating")
                    context.write(new Text(movieTitle), new Text(ageGroup + ":" + rating));
                }
            }
        }
    }

    public static class AgeGroupReducer extends Reducer<Text, Text, Text, Text> {
        
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            Map<String, Float> ageGroupSum = new HashMap<>();
            Map<String, Integer> ageGroupCount = new HashMap<>();
            
            String[] ageGroups = {"0-18", "18-35", "35-50", "50+"};
            for (String group : ageGroups) {
                ageGroupSum.put(group, 0.0f);
                ageGroupCount.put(group, 0);
            }

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                if (parts.length == 2) {
                    String ageGroup = parts[0];
                    float rating = Float.parseFloat(parts[1]);
                    
                    ageGroupSum.put(ageGroup, ageGroupSum.get(ageGroup) + rating);
                    ageGroupCount.put(ageGroup, ageGroupCount.get(ageGroup) + 1);
                }
            }

            StringBuilder result = new StringBuilder();
            for (int i = 0; i < ageGroups.length; i++) {
                String group = ageGroups[i];
                int count = ageGroupCount.get(group);
                
                if (i > 0) result.append(", ");
                
                if (count > 0) {
                    float avg = ageGroupSum.get(group) / count;
                    result.append(String.format("%s: %.2f", group, avg));
                } else {
                    result.append(String.format("%s: NA", group));
                }
            }
            context.write(key, new Text(result.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: Bai4 <movies_path> <users_path> <ratings_1_path> <ratings_2_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Age Group Rating Analysis");
        job.setJarByClass(Bai4.class);

        job.setMapperClass(AgeGroupMapper.class);
        job.setReducerClass(AgeGroupReducer.class);

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
