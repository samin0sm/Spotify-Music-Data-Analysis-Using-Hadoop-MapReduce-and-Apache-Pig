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

artist_tracks = FOREACH songs_no_header GENERATE
    FLATTEN(TOKENIZE(artists, ';')) AS artist,
    popularity;

artist_tracks = FILTER artist_tracks BY
    artist IS NOT NULL AND
    TRIM(artist) != '';

grouped_artists = GROUP artist_tracks BY TRIM(artist);

artist_stats = FOREACH grouped_artists GENERATE
    group AS artist,
    COUNT(artist_tracks) AS number_of_tracks,
    AVG(artist_tracks.popularity) AS average_popularity;

sorted_artists = ORDER artist_stats BY
    average_popularity DESC,
    artist ASC;

top10 = LIMIT sorted_artists 10;

STORE top10 INTO '/spotify/pig/output/top10_artists'
USING PigStorage('\t');
