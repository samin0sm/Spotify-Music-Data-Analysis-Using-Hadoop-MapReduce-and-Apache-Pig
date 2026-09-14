REGISTER '/home/sabit/pig/lib/piggybank.jar';

songs = LOAD '/spotify/input/spotify_tracks.csv'
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',')
AS (
    track_id:chararray,
    artists:chararray,
    album_name:chararray,
    track_name:chararray,
    popularity:int,
    duration_ms:int,
    explicit:boolean,
    danceability:double,
    energy:double,
    key:int,
    loudness:double,
    mode:int,
    speechiness:double,
    acousticness:double,
    instrumentalness:double,
    liveness:double,
    valence:double,
    tempo:double,
    time_signature:int,
    track_genre:chararray
);

songs_no_header = FILTER songs BY track_id != 'track_id';

grouped_explicit = GROUP songs_no_header BY explicit;

explicit_stats = FOREACH grouped_explicit GENERATE
    group AS explicit,
    COUNT(songs_no_header) AS number_of_tracks,
    AVG(songs_no_header.popularity) AS average_popularity,
    MAX(songs_no_header.popularity) AS max_popularity,
    MIN(songs_no_header.popularity) AS min_popularity;

sorted_explicit = ORDER explicit_stats BY explicit ASC;

STORE sorted_explicit
INTO '/spotify/pig/output/explicit_analysis'
USING PigStorage('\t');
