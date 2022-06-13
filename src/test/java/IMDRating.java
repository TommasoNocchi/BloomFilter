
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class IMDRating
{
    /*
     */
    public static class CountMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private final Text tconst  = new Text();
        private int averageRating = 0;
        private final Text rating = new Text();
        private int[] toWrite = new int[10];

        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final StringTokenizer rowIterator = new StringTokenizer(value.toString(),"\n");
            String[] rowFields;
            for(int i = 0; i < toWrite.length; i++)
                toWrite[i] = 0;
            while(rowIterator.hasMoreTokens()) {
                rowFields = rowIterator.nextToken().toString().split("\t");
                if (rowFields.length == 3) {
                    tconst.set(rowFields[0]);
                    averageRating = (int) Math.round(Double.parseDouble(rowFields[1]));
                    if (averageRating >= 1 && averageRating <=10)
                        toWrite[averageRating-1]++;
                }
            }
            for(int i = 0; i < toWrite.length; i++)
                context.write(new Text(String.format("%02d",i + 1)), new IntWritable(toWrite[i]));
        }
    }

    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(final Text key, final @NotNull Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int sum = 0;
            for (final IntWritable val : values)
                sum += val.get();
            context.write(key, new IntWritable(sum));
        }
    }

    public static class PopulateMapper extends Mapper<Object, Text, Text, Text>
    {
        private final Text tconst  = new Text();
        private int averageRating = 0;

        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final StringTokenizer rowIterator = new StringTokenizer(value.toString(),"\n");
            String[] rowFields;
            while(rowIterator.hasMoreTokens()) {
                rowFields = rowIterator.nextToken().toString().split("\t");
                if (rowFields.length == 3) {
                    tconst.set(rowFields[0]);
                    averageRating = (int) Math.round(Double.parseDouble(rowFields[1]));
                    if (averageRating >= 1 && averageRating <=10)
                        context.write(new Text(String.format("%02d",averageRating)), new Text(tconst));
                }
            }
        }
    }

    public static class PopulateReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(final Text key, final @NotNull Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            BloomFilter bloom_filter;
            int iter2 = 0;
            double p_falsePositiveRate = conf.getDouble("fpr",0.5);
            int n_itemNumber = conf.getInt(String.valueOf(key),0);

            bloom_filter = new BloomFilter(n_itemNumber,p_falsePositiveRate);
            for (final Text val : values) {
                iter2 += 1;
                bloom_filter.addItem(val);
            }
            context.write(key, new Text(bloom_filter.getString()));
        }
    }

    public static class CheckMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private Double p_FalsePositiveRate;
        private final Text tconst  = new Text();
        private int averageRating = 0;
        private int[] n_itemNumber = new int[10];
        private BloomFilter[] BF = new BloomFilter[10];

        public void setup(Mapper.Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            int i;
            p_FalsePositiveRate = conf.getDouble("fpr", 0.8);
            for(i = 0; i < 10; i++)
                n_itemNumber[i] = conf.getInt(String.format("%02d", i + 1), 0);

            FileSystem fs = FileSystem.get(conf);
            Path path2 = new Path("bloom/part-r-00000");
            BufferedReader br2 = new BufferedReader(new InputStreamReader(fs.open(path2)));
            String line;
            while((line = br2.readLine()) != null) {
                String[] tokens = line.split("\t");
                i = Integer.parseInt(tokens[0])-1;
                BF[i] = new BloomFilter(n_itemNumber[i], p_FalsePositiveRate);
                BF[i].setString(tokens[1]);
            }
        }

        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final StringTokenizer rowIterator = new StringTokenizer(value.toString(),"\n");
            String[] rowFields;
            while(rowIterator.hasMoreTokens()) {
                rowFields = rowIterator.nextToken().toString().split("\t");
                if (rowFields.length == 3) {
                    tconst.set(rowFields[0]);
                    averageRating = (int) Math.round(Double.parseDouble(rowFields[1]));
                    if (averageRating >= 1 && averageRating <=10)
                        for(int i = 0; i < 10; i++) {
                            if (i + 1 == averageRating)
                                continue;
                            if (BF[i].checkItem(tconst))
                                context.write(new Text(String.format("%02d", i + 1)), new IntWritable(1));
                        }
                }
            }
        }
    }

    public static class CheckReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        public void reduce(final Text key, final @NotNull Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int sum = 0, count = 0;
            for (final IntWritable val : values)
                sum += val.get();

            for(int i = 0; i < 10; i++)
                if(i + 1 != Integer.parseInt(key.toString()))
                    count += conf.getInt(String.format("%02d", i + 1), 0);

            context.write(new Text(key), new DoubleWritable((double)sum / (double)count));
        }
    }

    public static void main(final String @NotNull [] args) throws Exception {
        final Configuration jobConfiguration = new Configuration();
        Double fpr = (new Double(args[0]));
        jobConfiguration.setDouble("fpr",fpr);

        try (Job job = new Job(jobConfiguration, "count")) {
            job.setJarByClass(BloomFilter.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setMapperClass(CountMapper.class);
            job.setReducerClass(CountReducer.class);
            NLineInputFormat.addInputPath(job, new Path(args[1]));
            NLineInputFormat.setNumLinesPerSplit(job, 100_000);
            FileOutputFormat.setOutputPath(job, new Path(args[2]));

            job.waitForCompletion(true);
        }

        FileSystem fs = FileSystem.get(jobConfiguration);
        Path path = new Path("count/part-r-00000");
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;
        while((line = br.readLine()) != null) {
            String[] tokens = line.split("\t");
            jobConfiguration.setInt(tokens[0], Integer.parseInt(tokens[1]));
        }

        try (Job job = new Job(jobConfiguration, "count")) {
            job.setJarByClass(BloomFilter.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapperClass(PopulateMapper.class);
            job.setReducerClass(PopulateReducer.class);
            NLineInputFormat.addInputPath(job, new Path(args[1]));
            NLineInputFormat.setNumLinesPerSplit(job, 100_000);
            FileOutputFormat.setOutputPath(job, new Path(args[3]));

            job.waitForCompletion(true);
        }

        try (Job job = new Job(jobConfiguration, "check")) {
            job.setJarByClass(BloomFilter.class);
            job.setOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputValueClass(DoubleWritable.class);
            job.setMapperClass(CheckMapper.class);
            job.setReducerClass(CheckReducer.class);
            NLineInputFormat.addInputPath(job, new Path(args[1]));
            NLineInputFormat.setNumLinesPerSplit(job, 100_000);
            FileOutputFormat.setOutputPath(job, new Path(args[4]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}

/*{ // DEBUG
        FileSystem debugfs = FileSystem.get(conf);
        Path debugpath = new Path("debug/debug.txt");
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(debugfs.append(debugpath)))) {
        bw.write("CHECK MAPPER :" + Integer.toString(n_NumberOfKeys[i]) + " " + Integer.toString(m[i]) + "\n");
        }
} // DEBUG*/
