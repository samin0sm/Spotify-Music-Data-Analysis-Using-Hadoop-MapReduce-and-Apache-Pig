package com.neu.bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopPopularSongMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private final Text song = new Text();
    private final IntWritable popularity = new IntWritable();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip header
        if (line.startsWith("track_id,")) {
            return;
        }

        List<String> fields = parseCSVLine(line);

        // Spotify dataset has 20 columns
        if (fields.size() < 20) {
            return;
        }

        try {
            // Column 1 = artists
            // Column 4 = track_name
            // Column 5 = popularity
            String artists = fields.get(1).trim();
            String trackName = fields.get(3).trim();
            int pop = Integer.parseInt(fields.get(4).trim());

            song.set(trackName + "\t" + artists);
            popularity.set(pop);

            context.write(song, popularity);

        } catch (NumberFormatException e) {
            // Ignore invalid popularity values
        }
    }

    // CSV parser that handles commas inside quoted fields
    private List<String> parseCSVLine(String line) {

        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();

        boolean insideQuotes = false;

        for (int i = 0; i < line.length(); i++) {

            char c = line.charAt(i);

            if (c == '"') {

                // Handle escaped double quote ""
                if (insideQuotes && i + 1 < line.length()
                        && line.charAt(i + 1) == '"') {

                    current.append('"');
                    i++;

                } else {
                    insideQuotes = !insideQuotes;
                }

            } else if (c == ',' && !insideQuotes) {

                fields.add(current.toString());
                current.setLength(0);

            } else {

                current.append(c);
            }
        }

        fields.add(current.toString());

        return fields;
    }
}
