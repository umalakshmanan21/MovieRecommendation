# MovieRecommendation
Movie recommendation 
## Input files 
    Input file for movies : ex./user/itis6320/movies.txt
  • Input files for ratings : ex /user/itis6320/ratings.txt
  • 4 output folders ex : 0utput1,output2, output3, output4
  • Movie Name : "Amelie (Fabuleux destin d’Amelie Poulain)"
  • Minimum similarity correlation :ex 0.4
  • Minimum number of shared rating : ex. 10
  • Top n recommended movies : ex 10
In this program, there are 4 set of map reduce jobs being executed.
## Job 1:
  Mapper 1 takes movies.txt as input process the movie id, movie
name and generate (movieid ::moviename )as key,value respectively.
  Mapper 1A takes ratings.txt as input , processes them and generates
(movieid :: userid,ratings) as key value pairs respectively.
  Reducer 1 takes in this input and joins the userid, moviename,ratings,
thus generating (userid:: moviename,ratings) as key,value pairs.

## Job 2:
  Mapper 2 filters this key,value pair processes the userid, moviename
and ratings,it does not filter any value at this level.
  Reducer 2 : collects all the movies rated by a particular user and
generates, (userid:movie1,rating1,movie2,rating2..) as key value pair.

 ## Job 3:
  Mapper 3: This mapper process the movies into pairs recording the ratings
of each when a user has rated both movies. Care is taken at this stage not
to repeat the pairs or use same movie pairing with itself. Now, movie pair
and associated rating are generated as key value pairs. At this stage,
shared rating between the movie pair is calculated and all the pairs that do
not qualify the minimum shared rating requirement is eliminated.
  Reducer 3: This reducer processes the movie pairs and theirs ratings as
vectors for calculation of similarity metrics.

## Job 4:
  Mapper 4: The similarity correlation and cosine correlation for the two rating
vectors for each movie pair is calculated. At this stage, the any movie pair
that do not qualify minimum similarity value and top n recommendations for
all the movie pairs is calculated. Finally, the movie mentioned in the
argument is filtered and mentioned “n” number of similar movies with
similarity greater than the specified threshold is generated.
  Reducer 4: Recommended movie name as key and their correlation and
cosine similarities, similarity ratio and number of ratings shared as values
are generated and output is stored.

## output format:
Key : Movie name 
Value : similar movie name , blended similarity metric, cosine similarity , correlation , number of ratings shared. 





