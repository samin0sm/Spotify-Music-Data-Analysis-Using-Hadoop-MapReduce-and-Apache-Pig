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

genre_group = GROUP songs_no_header BY track_genre;

genre_stats = FOREACH genre_group GENERATE
    group AS genre,
    COUNT(songs_no_header) AS number_of_tracks,
    AVG(songs_no_header.popularity) AS average_popularity,
    MAX(songs_no_header.popularity) AS maximum_popularity,
    MIN(songs_no_header.popularity) AS minimum_popularity;

sorted_genres = ORDER genre_stats BY genre ASC;

STORE sorted_genres INTO '/spotify/pig/output/genre_statistics'
USING PigStorage('\t');
