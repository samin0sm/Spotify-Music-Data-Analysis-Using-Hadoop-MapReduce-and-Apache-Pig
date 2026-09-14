package com.neu.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopPopularSongReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Text topSong = new Text();
    private int maxPopularity = -1;

    @Override
    protected void reduce(
            Text key,
            Iterable<IntWritable> values,
            Context context)
            throws IOException, InterruptedException {

        for (IntWritable value : values) {

            int popularity = value.get();

            if (popularity > maxPopularity) {

                maxPopularity = popularity;
                topSong.set(key);
            }
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        if (maxPopularity >= 0) {
            context.write(
                topSong,
                new IntWritable(maxPopularity)
            );
        }
    }
}
