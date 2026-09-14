package com.neu.bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopArtistsMapper
        extends Mapper<Object, Text, Text, Text> {

    private final Text outputKey = new Text("ALL");
    private final Text outputValue = new Text();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip CSV header
        if (line.startsWith("track_id,artists,")) {
            return;
        }

        String[] fields = parseCSVLine(line);

        // artists = column 1
        // popularity = column 4
        if (fields.length <= 4) {
            return;
        }

        String artistsField = fields[1].trim();
        String popularityText = fields[4].trim();

        if (artistsField.isEmpty()) {
            return;
        }

        try {

            int popularity = Integer.parseInt(popularityText);

            // Extract individual artists
            List<String> artists =
                    parseArtists(artistsField);

            for (String artist : artists) {

                artist = artist.trim();

                if (!artist.isEmpty()) {

                    outputValue.set(
                            artist + "\t" + popularity
                    );

                    context.write(
                            outputKey,
                            outputValue
                    );
                }
            }

        } catch (NumberFormatException e) {
            // Ignore invalid popularity values
        }
    }

    /*
     * Extract individual artists.
     *
     * Handles:
     *
     * Sam Smith;Kim Petras
     *
     * and:
     *
     * ['Sam Smith', 'Kim Petras']
     */
    private List<String> parseArtists(String text) {

        List<String> artists =
                new ArrayList<>();

        text = text.trim();

        // Remove surrounding brackets
        if (text.startsWith("[") &&
            text.endsWith("]")) {

            text = text.substring(
                    1,
                    text.length() - 1
            );
        }

        /*
         * Dataset/output may contain artists separated
         * by semicolons.
         */
        if (text.contains(";")) {

            String[] parts =
                    text.split(";");

            for (String part : parts) {

                part = cleanArtist(part);

                if (!part.isEmpty()) {
                    artists.add(part);
                }
            }

            return artists;
        }

        /*
         * Handle Python-list style:
         *
         * ['Artist 1', 'Artist 2']
         */
        boolean insideQuotes = false;
        char quoteChar = 0;

        StringBuilder current =
                new StringBuilder();

        for (int i = 0; i < text.length(); i++) {

            char c = text.charAt(i);

            if (c == '\'' || c == '"') {

                if (!insideQuotes) {

                    insideQuotes = true;
                    quoteChar = c;

                } else if (c == quoteChar) {

                    insideQuotes = false;

                } else {

                    current.append(c);
                }

            }
            else if (c == ',' && !insideQuotes) {

                String artist =
                        cleanArtist(
                                current.toString()
                        );

                if (!artist.isEmpty()) {
                    artists.add(artist);
                }

                current.setLength(0);

            }
            else {

                current.append(c);
            }
        }

        String artist =
                cleanArtist(
                        current.toString()
                );

        if (!artist.isEmpty()) {
            artists.add(artist);
        }

        return artists;
    }

    /*
     * Clean brackets, quotes and extra spaces.
     */
    private String cleanArtist(String artist) {

        artist = artist.trim();

        artist = artist.replaceAll(
                "^['\"]+|['\"]+$",
                ""
        );

        artist = artist.trim();

        return artist;
    }

    /*
     * CSV parser that correctly handles commas
     * inside quoted fields.
     */
    private String[] parseCSVLine(String line) {

        List<String> fields =
                new ArrayList<>();

        StringBuilder current =
                new StringBuilder();

        boolean insideQuotes = false;

        for (int i = 0; i < line.length(); i++) {

            char c = line.charAt(i);

            if (c == '"') {

                if (insideQuotes &&
                    i + 1 < line.length() &&
                    line.charAt(i + 1) == '"') {

                    current.append('"');
                    i++;

                } else {

                    insideQuotes =
                            !insideQuotes;
                }

            }
            else if (c == ',' && !insideQuotes) {

                fields.add(
                        current.toString()
                );

                current.setLength(0);

            }
            else {

                current.append(c);
            }
        }

        fields.add(
                current.toString()
        );

        return fields.toArray(
                new String[0]
        );
    }
}
