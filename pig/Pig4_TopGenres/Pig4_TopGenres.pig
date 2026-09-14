genre_stats = LOAD '/spotify/pig/output/genre_statistics'
USING PigStorage('\t')
AS (
    genre:chararray,
    track_count:int,
    average_popularity:double,
    max_popularity:int,
    min_popularity:int
);

sorted_genres = ORDER genre_stats BY average_popularity DESC;

top10_genres = LIMIT sorted_genres 10;

STORE top10_genres
INTO '/spotify/pig/output/top10_genres'
USING PigStorage('\t');
