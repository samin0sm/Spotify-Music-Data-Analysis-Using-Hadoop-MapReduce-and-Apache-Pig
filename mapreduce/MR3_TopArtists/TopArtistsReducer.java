package com.neu.bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopArtistsReducer
        extends Reducer<Text, Text, Text, Text> {

    private static class ArtistStats {

        int count;
        long totalPopularity;

        ArtistStats() {
            count = 0;
            totalPopularity = 0;
        }

        void add(int popularity) {
            count++;
            totalPopularity += popularity;
        }

        double average() {
            return (double) totalPopularity / count;
        }
    }

    @Override
    public void reduce(Text key,
                        Iterable<Text> values,
                        Context context)
            throws IOException, InterruptedException {

        Map<String, ArtistStats> artists = new HashMap<>();

        for (Text value : values) {

            String[] parts =
                    value.toString().split("\t", 2);

            if (parts.length != 2) {
                continue;
            }

            String artist = parts[0].trim();

            try {

                int popularity =
                        Integer.parseInt(parts[1].trim());

                ArtistStats stats =
                        artists.get(artist);

                if (stats == null) {
                    stats = new ArtistStats();
                    artists.put(artist, stats);
                }

                stats.add(popularity);

            } catch (NumberFormatException e) {
                // Ignore invalid popularity values
            }
        }

        // Sort artists by average popularity, highest first
        List<Map.Entry<String, ArtistStats>> ranking =
                new ArrayList<>(artists.entrySet());

        ranking.sort(
            Comparator
                .<Map.Entry<String, ArtistStats>>comparingDouble(
                    entry -> entry.getValue().average()
                )
                .reversed()
                .thenComparing(
                    Map.Entry::getKey
                )
        );

        // Output only Top 10
        int limit = Math.min(10, ranking.size());

        for (int i = 0; i < limit; i++) {

            Map.Entry<String, ArtistStats> entry =
                    ranking.get(i);

            String artist = entry.getKey();
            ArtistStats stats = entry.getValue();

            context.write(
                    new Text(artist),
                    new Text(
                            stats.count +
                            "\t" +
                            String.format(
                                    "%.2f",
                                    stats.average()
                            )
                    )
            );
        }
    }
}
