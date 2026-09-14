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

grouped_songs = GROUP songs_no_header BY track_id;

unique_songs = FOREACH grouped_songs GENERATE
    group AS track_id,
    MAX(songs_no_header.track_name) AS track_name,
    MAX(songs_no_header.artists) AS artists,
    MAX(songs_no_header.popularity) AS popularity,
    MAX(songs_no_header.track_genre) AS track_genre;

sorted_songs = ORDER unique_songs BY popularity DESC, track_name ASC;

top10 = LIMIT sorted_songs 10;

result = FOREACH top10 GENERATE
    track_name,
    artists,
    popularity,
    track_genre;

STORE result INTO '/spotify/pig/output/top10_songs'
USING PigStorage('\t');
