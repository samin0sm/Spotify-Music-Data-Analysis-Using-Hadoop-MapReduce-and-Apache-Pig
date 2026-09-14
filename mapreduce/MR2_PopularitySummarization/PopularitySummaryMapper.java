package com.neu.bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PopularitySummaryMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private final Text genre = new Text();
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
            // Column 5 = popularity
            // Column 20 = track_genre
            int pop = Integer.parseInt(fields.get(4).trim());
            String trackGenre = fields.get(19).trim();

            if (!trackGenre.isEmpty()) {
                genre.set(trackGenre);
                popularity.set(pop);

                context.write(genre, popularity);
            }

        } catch (NumberFormatException e) {
            // Ignore invalid popularity values
        }
    }

    private List<String> parseCSVLine(String line) {

        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();

        boolean insideQuotes = false;

        for (int i = 0; i < line.length(); i++) {

            char c = line.charAt(i);

            if (c == '"') {

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
