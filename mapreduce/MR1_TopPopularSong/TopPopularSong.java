package com.neu.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopPopularSong {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {

            System.err.println(
                "Usage: TopPopularSong <input> <output>"
            );

            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
            conf,
            "Top Popular Song"
        );

        job.setJarByClass(TopPopularSong.class);

        job.setMapperClass(
            TopPopularSongMapper.class
        );

        job.setReducerClass(
            TopPopularSongReducer.class
        );

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // One reducer guarantees one global maximum
        job.setNumReduceTasks(1);

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
