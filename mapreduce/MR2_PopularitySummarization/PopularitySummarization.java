package com.neu.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PopularitySummarization {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {

            System.err.println(
                "Usage: PopularitySummarization <input> <output>"
            );

            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
            conf,
            "Spotify Popularity Summarization"
        );

        job.setJarByClass(
            PopularitySummarization.class
        );

        job.setMapperClass(
            PopularitySummaryMapper.class
        );

        job.setReducerClass(
            PopularitySummaryReducer.class
        );

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(
            job,
            new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
            job,
            new Path(args[1])
        );

        System.exit(
            job.waitForCompletion(true) ? 0 : 1
        );
    }
}
