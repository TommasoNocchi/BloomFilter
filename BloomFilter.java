package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.hash.MurmurHash;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.StringTokenizer;

public class BloomFilter
{

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
		        /*
		         * we have to test whether the average Rating is a well formed double
		         */
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

    public static class BloomMapper extends Mapper<Object, Text, Text, Text>
    {
        private Double p_FalsePositiveRate;
        private int k_HashFunctionsNumber;

        public void setup(Mapper.Context context) throws IOException {
            Configuration conf = context.getConfiguration();

            p_FalsePositiveRate = conf.getDouble("fpr", 0.5);
            k_HashFunctionsNumber = (int)Math.ceil(- Math.log(p_FalsePositiveRate) / Math.log(2));
        }

        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final Text tconst  = new Text();
            int averageRating = 0;
            final StringTokenizer rowIterator = new StringTokenizer(value.toString(),"\n");
            String[] rowFields;
            Text ratingText, idText;

            while(rowIterator.hasMoreTokens()) {
                rowFields = rowIterator.nextToken().toString().split("\t");
                if (rowFields.length == 3) {
                    tconst.set(rowFields[0]);
                    averageRating = (int) Math.round(Double.parseDouble(rowFields[1]));
                    ratingText.set(String.format("%02d",averageRating));
                    idText.set(rowFields[0]);
                    context.write(ratingText, idText);
/*                    for (int i = 0; i < k_HashFunctionsNumber; i++) {
                        bitPosition = new MurmurHash().hash(tconst.getBytes(), tconst.getBytes().length, i);
                        context.write(new Text(String.format("%02d",averageRating)), new Text(Integer.toString(bitPosition)));
                    }
 */
                }
            }
        }
    }

    public static class BloomReducer extends Reducer<Text, Text, Text, Text> {
        private int n_NumberOfKeys;
        private int m_NumberOfBits;
        private BitArray bitArray;
        public void reduce(final Text key, final @NotNull Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            double p_FalsePositiveRate;

            n_NumberOfKeys = conf.getInt(key.toString(), 1);
            p_FalsePositiveRate = conf.getDouble("fpr", 0.5);
            m_NumberOfBits = (int)Math.round(-(n_NumberOfKeys * Math.log(p_FalsePositiveRate) / Math.pow(Math.log(2), 2)));
            bitArray = new BitArray(m_NumberOfBits);
            for (Text val : values) {
                String bitPosition = val.toString();
                int mod = ((Integer. parseInt(bitPosition) % m_NumberOfBits) +m_NumberOfBits) % m_NumberOfBits;
                bitArray.setBit(mod, true);
            }
            context.write(new Text(key.toString()),new Text(bitArray.getString()));
        }
    }

    public static class CheckMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private Double p_FalsePositiveRate;
        private final Text tconst  = new Text();
        private int averageRating = 0;
        private int k_HashFunctionsNumber;
        private int[] m = new int[10];
        private int[] n_NumberOfKeys = new int[10];
        private int[] paolo = new int[10];
        private BitArray[] bitArrays = new BitArray[10];

        public void setup(Mapper.Context context) throws IOException {
            Configuration conf = context.getConfiguration();

            FileSystem fs = FileSystem.get(conf);
            Path path2 = new Path("bloom/part-r-00000");
            BufferedReader br2 = new BufferedReader(new InputStreamReader(fs.open(path2)));
            while(true) {
                String line;
                line = br2.readLine();
                if(line == null)
                    break;
                String[] tokens = line.split("\t");
                bitArrays[Integer.parseInt(tokens[0])-1] = new BitArray(tokens[1]);
            }

            p_FalsePositiveRate = conf.getDouble("fpr", 0.8);
            k_HashFunctionsNumber = (int)Math.ceil(-Math.log(p_FalsePositiveRate) / Math.log(2));
            for(int i=0; i<10; i++){
                n_NumberOfKeys[i] = conf.getInt(String.format("%02d", i + 1), 0);
                m[i] = (int)Math.ceil(-Math.round((n_NumberOfKeys[i] * Math.log(p_FalsePositiveRate) / Math.pow(Math.log(2), 2))));
}
        }

        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            final StringTokenizer rowIterator = new StringTokenizer(value.toString(),"\n");
            String[] rowFields;
            int bitPosition;
            boolean isFalsePositive;
            while(rowIterator.hasMoreTokens()) {
                rowFields = rowIterator.nextToken().toString().split("\t");
                if (rowFields.length == 3) {
                    tconst.set(rowFields[0]);
                    averageRating = (int) Math.round(Double.parseDouble(rowFields[1]));
                    for(int i = 0; i < 10; i++){
                        if(i + 1 == averageRating || m[i] == 0)
                            continue;
                        isFalsePositive = true;
                        for (int ii = 0; ii < k_HashFunctionsNumber && isFalsePositive; ii++) {
                            bitPosition = new MurmurHash().hash(tconst.getBytes(), tconst.getBytes().length, ii);
                            int mod = ((bitPosition % m[i]) + m[i]) % m[i];
                            isFalsePositive &= bitArrays[i].getBit(mod);
                        }
                        if(isFalsePositive) {
                            context.write(new Text(String.format("%02d", i + 1)), new IntWritable(1));
                        }
                    }
                }
            }
        }
    }

    public static class CheckReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(final Text key, final @NotNull Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            int count = 0;
            for (final IntWritable val : values)
                count++;
            int intKey = Integer.parseInt(key.toString());
            context.write(new Text(String.format("%02d",intKey)), new IntWritable(count));
        }
    }

    public static void main(final String @NotNull [] args) throws Exception {
        final Configuration countConfiguration = new Configuration();
        try (Job job = new Job(countConfiguration, "count")) {
            job.setJarByClass(BloomFilter1.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setMapperClass(CountMapper.class);
            job.setReducerClass(CountReducer.class);
            NLineInputFormat.addInputPath(job, new Path(args[1]));
            NLineInputFormat.setNumLinesPerSplit(job, 100_000);
            FileOutputFormat.setOutputPath(job, new Path(args[2]));

            job.waitForCompletion(true);
        }
        Double fpr = (new Double(args[0]));
        final Configuration bloomConfiguration = new Configuration();

        FileSystem fs = FileSystem.get(bloomConfiguration);
        Path path = new Path("count/part-r-00000");
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        while(true) {
            String line;
            line = br.readLine();
            if(line == null)
                break;
            String[] tokens = line.split("\t");
            bloomConfiguration.setInt(tokens[0], Integer.parseInt(tokens[1]));
        }

        bloomConfiguration.setDouble("fpr", fpr);
        try (Job job = new Job(bloomConfiguration, "bloom")) {
           job.setJarByClass(BloomFilter1.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapperClass(BloomMapper.class);
            job.setReducerClass(BloomReducer.class);
            NLineInputFormat.addInputPath(job, new Path(args[1]));
            NLineInputFormat.setNumLinesPerSplit(job, 100_000);
            FileOutputFormat.setOutputPath(job, new Path(args[3]));
            job.waitForCompletion(true);
        }

        try (Job job = new Job(bloomConfiguration, "check")) {
            job.setJarByClass(BloomFilter1.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
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
