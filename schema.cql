CREATE KEYSPACE killr_video WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 3};
USE killr_video;

CREATE TABLE movies_by_actor (
  actor TEXT,
  release_year INT,
  movie_id UUID,
  title TEXT,
  genres SET<TEXT>,
  rating FLOAT,
  PRIMARY KEY ((actor), release_year, movie_id)
) WITH CLUSTERING ORDER BY (release_year DESC, movie_id ASC);

CREATE TABLE movies (
  movie_id UUID,
  title TEXT,
  release_year INT,
  PRIMARY KEY ((movie_id))
);

CREATE TABLE movie_genres (
  movie_id UUID,
  genres SET<TEXT>,
  PRIMARY KEY ((movie_id))
);

CREATE TABLE favorite_movies (
  title TEXT,
  release_year INT,
  rating FLOAT,
  genres SET<TEXT>,
  PRIMARY KEY ((title, release_year))
);
