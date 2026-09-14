package com.neu.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PopularitySummaryReducer
        extends Reducer<Text, IntWritable, Text, Text> {

    @Override
    protected void reduce(
            Text key,
            Iterable<IntWritable> values,
            Context context)
            throws IOException, InterruptedException {

        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        long sum = 0;
        long count = 0;

        for (IntWritable value : values) {

            int popularity = value.get();

            if (popularity < min) {
                min = popularity;
            }

            if (popularity > max) {
                max = popularity;
            }

            sum += popularity;
            count++;
        }

        if (count > 0) {

            double average = (double) sum / count;

            String result =
                    min + "\t" +
                    max + "\t" +
                    String.format("%.2f", average) + "\t" +
                    count;

            context.write(key, new Text(result));
        }
    }
}
