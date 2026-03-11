import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai2 {

    public static class GenreMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private Map<String, String> movieGenres = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                BufferedReader reader = new BufferedReader(new FileReader("movies.txt"));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] tokens = line.split(", ");
                    if (tokens.length >= 3) {
                        String movieID = tokens[0].trim();
                        String genres = tokens[2].trim(); 
                        movieGenres.put(movieID, genres);
                    }
                }
                reader.close();
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(", ");
            if (tokens.length >= 3) {
                String movieID = tokens[1].trim();
                float rating = Float.parseFloat(tokens[2].trim());
                
                String genres = movieGenres.get(movieID);
                if (genres != null) {
                    String[] genreList = genres.split("\\|");
                    for (String genre : genreList) {
                        context.write(new Text(genre.trim()), new FloatWritable(rating));
                    }
                }
            }
        }
    }

    public static class GenreReducer extends Reducer<Text, FloatWritable, Text, Text> {
        
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
                throws IOException, InterruptedException {
            int count = 0;
            float sum = 0;

            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }

            float average = sum / count;
            
            String result = String.format("Avg: %.2f (Count: %d)", average, count);
            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: Bai2 <movies_path> <ratings_1_path> <ratings_2_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Genre Rating Analysis");
        job.setJarByClass(Bai2.class);

        job.setMapperClass(GenreMapper.class);
        job.setReducerClass(GenreReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.addCacheFile(new Path(args[0]).toUri());

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
