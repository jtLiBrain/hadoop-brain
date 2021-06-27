package org.jtLiBrain.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.jtLiBrain.hadoop.common.io.TextPair;

import java.io.IOException;

public class SecondarySort {
    public static class MapClass extends Mapper<LongWritable, Text, TextPair, Text> {
        private final TextPair key = new TextPair();

        @Override
        public void map(LongWritable inKey, Text inValue,
                        Context context) throws IOException, InterruptedException {
            String[] arr = inValue.toString().split(",");

            key.set(arr[0], arr[1]);

            context.write(key, inValue);
        }
    }

    public static class ReduceClass extends Reducer<TextPair, Text, NullWritable, Text> {
        @Override
        protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value: values) {
                context.write(NullWritable.get(), value);
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: secondarysort <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "secondary sort");
        job.setJarByClass(SecondarySort.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        // sort by the first and second in the pair
        job.setSortComparatorClass(TextPair.NaturalComparator.class);
        // group and partition by the first int in the pair
        job.setPartitionerClass(TextPair.FirstPartitioner.class);
        job.setGroupingComparatorClass(TextPair.FirstGroupingComparator.class);

        // the map output is TextPair, Text
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(Text.class);

        // the reduce output is NullWritable, Text
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(2);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
