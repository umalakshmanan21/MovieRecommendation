package PackageJoin;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import java.util.*;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;

public class MultipleFiles extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new MultipleFiles(), args);
        System.exit(res);

    }

    public int run(String[] args) throws Exception {
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path p1 = new Path(files[0]);//movie file
        Path p2 = new Path(files[1]);//rating file
        Path p3 = new Path(files[2]);//output1 - join file - inout for mapper 2
        Path p4 = new Path(files[3]);//output 2 - input for mapper 3
        Path p5 = new Path(files[4]);//output 3 - input for next mapper
        Path p6 = new Path(files[5]);
        String M = args[6];
        String S = args[7];
        String R = args[8];
        String I = args[9];
        //Path p7 = new Path(files[6]);

        FileSystem fs = FileSystem.get(c);
        boolean success1 = false;

        Job job = new Job(c, "Multiple Job");

        job.setJarByClass(MultipleFiles.class);
        MultipleInputs.addInputPath(job, p1, TextInputFormat.class, MultipleMap1.class);
        MultipleInputs.addInputPath(job, p2, TextInputFormat.class, MultipleMap2.class);
        job.setReducerClass(MultipleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, p3);
        boolean success = job.waitForCompletion(true);

        if (success) {
            Job job2 = new Job(c, "Multiple Job2");
            job2.setJarByClass(MultipleFiles.class);
            MultipleInputs.addInputPath(job2, p3, TextInputFormat.class, MultipleMap3.class);
            job2.setReducerClass(MultipleReducer1.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(job2, p4);
            success1 = job2.waitForCompletion(true);
        }

        if (success) {
            Job job3 = new Job(c, "Multiple Job3");
            job3.setJarByClass(MultipleFiles.class);
            MultipleInputs.addInputPath(job3, p4, TextInputFormat.class, MultipleMap4.class);
            job3.setReducerClass(MultipleReducer2.class);
            job3.setOutputKeyClass(Text.class);
            Configuration conf = job3.getConfiguration();
            conf.set("raingshared", R);
            job3.setOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(job3, p5);
            success1 = job3.waitForCompletion(true);
        }

        if (success) {
            Job job4 = new Job(c, "Multiple Job3");
            job4.setJarByClass(MultipleFiles.class);
            MultipleInputs.addInputPath(job4, p5, TextInputFormat.class, MultipleMap5.class);
            job4.setReducerClass(MultipleReducer3.class);
            Configuration conf = job4.getConfiguration();
            conf.set("moviename", M);
            conf.set("similaritymetric", S);
            conf.set("numberofitems", I);
            job4.setOutputKeyClass(Text.class);
            job4.setOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(job4, p6);
            success1 = job4.waitForCompletion(true);
        }

        return success1 ? 0 : 1;
    }
// MultipleMap1,multiplemap2,multiplereducer - does the step 1 task of joining the movie and rating 
// files and outputs data that gives all the movies rated by each user. 

    public static class MultipleMap1 extends Mapper<LongWritable, Text, Text, Text> {

        Text keyEmit = new Text();
        Text valEmit = new Text();
        private String filetag = "A~";

        @Override
        public void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(",");
            keyEmit.set(words[0]);
            valEmit.set(filetag + words[1]);
            context.write(keyEmit, valEmit);
        }
    }

    public static class MultipleMap2 extends Mapper<LongWritable, Text, Text, Text> {

        Text keyEmit = new Text();
        Text valEmit = new Text();
        private String filetag = "B~";

        public void map(LongWritable k, Text v, Context context) throws IOException, InterruptedException {
            String line = v.toString();
            String[] words = line.split(",");
            keyEmit.set(words[1]);
            valEmit.set(filetag + words[0] + "," + words[2]);
            context.write(keyEmit, valEmit);
        }
    }

    public static class MultipleReducer extends Reducer<Text, Text, Text, Text> {

        Text valEmit = new Text();
        Text keyemit = new Text();
        String merge = "";
        private String movieName;
        private String userID;
        private String rating;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();
            int i = 0;
            for (Text value : values) {
                String currValue = value.toString();
                String splitVals[] = currValue.split("~");
                if (splitVals[0].equals("A")) {
                    movieName = splitVals[1];

                } else if (splitVals[0].equals("B")) {
                    String splitVals1[] = splitVals[1].split(",");
                    userID = splitVals1[0];
                    rating = splitVals1[1];
                    sb.append(splitVals1[0] + "::" + splitVals1[1]).append(",");
                }
            }

            String[] array = sb.toString().split(",");
            for (String s : array) {
                String[] arr1 = s.split("::");
                keyemit.set(movieName);
                if (arr1.length == 2) {
                    context.write(keyemit, new Text("::" + arr1[0] + "::" + arr1[1]));
                }
            }
        }
    }

    public static class MultipleMap3 extends Mapper<LongWritable, Text, Text, Text> {

        Text keyEmit = new Text();
        Text valEmit = new Text();

        @Override
        public void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split("::");
            keyEmit.set(words[1]);
            valEmit.set("::" + words[0].trim() + "," + words[2]);
            context.write(keyEmit, valEmit);
        }
    }

    public static class MultipleReducer1 extends Reducer<Text, Text, Text, Text> {

        Text valEmit = new Text();
        Text keyemit = new Text();
        String merge = "";
        private String movieName;
        private String userID;
        private String rating1;
        private String rating;
        private String rating2;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int i = 0;
            Configuration conf = context.getConfiguration();
            String movieName = conf.get("moviename");
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                String currValue = value.toString();
                String splitVals[] = currValue.split("::");
                sb.append(splitVals[1]).append(";;");
            }
            keyemit.set(key.toString());
            context.write(keyemit, new Text("::" + sb.substring(0, sb.length() - 2)));
        }
    }

    public static class MultipleMap4 extends Mapper<LongWritable, Text, Text, Text> {

        Text keyEmit = new Text();
        Text valEmit = new Text();

        @Override
        public void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] val = line.split("::");
            String[] words = val[1].split(";;");
            StringBuilder sb = new StringBuilder();
            if (words.length >= 2) {
                ArrayList<Integer> rateA = new ArrayList<Integer>();
                ArrayList<Integer> rateB = new ArrayList<Integer>();

                for (int i = 0; i < words.length; i++) {
                    for (int j = (i + 1); j < words.length; j++) {
                        String[] movRatI = words[i].split(",");
                        String[] movRatJ = words[j].split(",");
                        if (!movRatI[0].equals(movRatJ[0])) {
                            keyEmit.set("(" + movRatI[0] + "," + movRatJ[0] + ")");
                            valEmit.set("(" + movRatI[1] + "," + movRatJ[1] + ")");
                            //valEmit.set(",  (" + rateA + "," + rateB + ")");                         
                            context.write(keyEmit, valEmit);
                        }

                    }
                }
            }
        }
    }

    public static class MultipleReducer2 extends Reducer<Text, Text, Text, Text> {

        Text valEmit = new Text();
        Text keyemit = new Text();
        String merge = "";
        private String movieName;
        private String userID;
        private String rating1;
        private String rating;
        private String rating2;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int i = 0;
            int j = 0;
            int[] val1 = null;
            StringBuilder sb = new StringBuilder();
            ArrayList<Double> rateA = new ArrayList<Double>();
            ArrayList<Double> rateB = new ArrayList<Double>();
            Configuration conf1 = context.getConfiguration();
            Integer raingshared = Integer.parseInt(conf1.get("raingshared"));

            for (Text value : values) {
                String currValue = value.toString();
                String[] ratval1 = currValue.split("\\(");
                String[] ratval2 = ratval1[1].split(",");
                ratval2[0] = ratval2[0].replaceAll("[^0-9\\.]+", "");
                ratval2[1] = ratval2[1].replaceAll("[^0-9\\.]+", "");

                Double ratevalueA = Double.parseDouble(ratval2[0]);
                Double ratevalueB = Double.parseDouble(ratval2[1]);
                rateA.add(ratevalueA);
                rateB.add(ratevalueB);
                j++;

            }
            if (j >= raingshared) {
                keyemit.set(key.toString());
                context.write(keyemit, new Text("::" + rateA + "," + rateB));
            }

        }
    }

    public static class MultipleMap5 extends Mapper<LongWritable, Text, Text, Text> {

        Text keyEmit = new Text();
        Text valEmit = new Text();
        private String movieName;
        private String userID;
        private String rating1;
        private String rating;
        private String rating2;
        private static Map<String, String> movieMap = new HashMap();

        @Override
        public void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException {
            StringBuilder rate1 = new StringBuilder();
            StringBuilder rate2 = new StringBuilder();
            ArrayList<Double> rateAdou = new ArrayList<>();
            ArrayList<Double> rateBdou = new ArrayList<>();

            String line = value.toString();
            String[] val = line.split("::");
            String[] words = val[1].split("\\],");

            Double[] rateA = new Double[words.length];
            Double[] rateB = new Double[words.length];

            String[] rateA1 = words[0].split(",");
            String[] rateB1 = words[1].split(",");
            int sharedRating = rateB1.length;
            for (int j = 0; j < rateA1.length; j++) {
                rateAdou.add(Double.parseDouble(rateA1[j].replaceAll("[^0-9\\.]+", "")));
            }
            for (int j = 0; j < rateB1.length; j++) {
                rateBdou.add(Double.parseDouble(rateB1[j].replaceAll("[^0-9\\.]+", "")));
            }
            double correlation = Correlation(rateAdou, rateBdou);
            double cosinesimilarity = cosineSimilarity(rateAdou, rateBdou);
            double statisticalCorrelation = 0.5 * (correlation + cosinesimilarity);
             Configuration conf = context.getConfiguration();
            String movieName = conf.get("moviename");
            String similaritymetric = conf.get("similaritymetric");
            Double similaritymetricD = Double.parseDouble(similaritymetric);
            String numberofitems = conf.get("numberofitems");
            if (val[0].contains(movieName) && ( similaritymetricD <= statisticalCorrelation)){
                String suggestedMovie = val[0].replace(movieName, "").replace(",", "");
                keyEmit.set(movieName);
                context.write(keyEmit, new Text("::"+suggestedMovie+","+correlation + ", " + cosinesimilarity + "," + statisticalCorrelation + "," + sharedRating));
            }
        }

        public double Correlation(ArrayList<Double> xs, ArrayList<Double> ys) {
            //TODO: check here that arrays are not null, of the same length etc

            double sx = 0.0;
            double sy = 0.0;
            double sxx = 0.0;
            double syy = 0.0;
            double sxy = 0.0;

            int n = xs.size();

            for (int i = 0; i < n; ++i) {
                Double x = xs.get(i);
                Double y = ys.get(i);

                sx += x;
                sy += y;
                sxx += x * x;
                syy += y * y;
                sxy += x * y;
            }

            // covariation
            double cov = sxy / n - sx * sy / n / n;
            // standard error of x
            double sigmax = Math.sqrt(sxx / n - sx * sx / n / n);
            // standard error of y
            double sigmay = Math.sqrt(syy / n - sy * sy / n / n);

            // correlation is just a normalized covariation
            return cov / sigmax / sigmay;
        }

        public static double cosineSimilarity(ArrayList<Double> vectorA, ArrayList<Double> vectorB) {
            double dotProduct = 0.0;
            double normA = 0.0;
            double normB = 0.0;
            for (int i = 0; i < vectorA.size(); i++) {
                dotProduct += vectorA.get(i) * vectorB.get(i);
                normA += Math.pow(vectorA.get(i), 2);
                normB += Math.pow(vectorB.get(i), 2);
            }
            return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
        }

    }
    static class ValueComparator implements Comparator {

        Map map;

        public ValueComparator(Map map) {
            this.map = map;
        }

        public int compare(Object valueA, Object valueB) {
            
            String[] KeyA1 = valueA.toString().split(",");
            Double valA = Double.parseDouble(KeyA1[3]);
            String[] KeyB1 = valueB.toString().split(",");
            Double valB= Double.parseDouble(KeyB1[3]);
           

            if (valA == valB || valB > valA) {
                return 1;
            } else {
                return -1;
            }

        }
    }

    public static class MultipleReducer3 extends Reducer<Text, Text, Text, Text> {

        Text valEmit = new Text();
        Text keyemit = new Text();
        String merge = "";
        private String movieName;
        private String userID;
        private String rating1;
        private String rating;
        private String rating2;
        private static Map<String,String> movieMap = new HashMap();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int i = 0;
           
            for (Text value : values) {
                
                
              context.write(key, value);  

            }
        }

    }
}
