# Similar Users Recommendation System
Recommendation System for finding top 5 similar users

- Developed recommendation system for finding top 5 similar users for each user based on the fraction of movies they have watched in common.
- Calculated minhash signatures for each user (represented as a set of movies) and applied LSH (Locality-sensitive hashing) for finding candidate pairs of similar users.
- Computed Jaccard similarity of each candidate pair to find the top 5 similar users for each user.

Core Technology: Apache Spark, Python, AWS (Amazon EC2).

# Program
Test set contains 100 different movies, numbered from 0 to 99. A user is represented as a set of movies. Jaccard coefficient is used to measure the similarity of sets.

- Applied minhash to obtain a signature of 20 values for each user.
- i-th hash function for the signature is given as : 
```
h(x,i) = (3x + 13i) % 100, where x is the original row number in the matrix. 
```

- Applied LSH to speed up the process of finding similar users, where the signature is divided into 5 bands, with 4 values in each band.
- Finding similar users: Based on the LSH result, for each user U, the program finds top-5 users who are most similar to U (by their Jaccard similarity). If there aren’t 5 similar users, e.g. U2 is only similar with U5, U9, U10, the program just outputs *U2:U5,U9,U10*.

# Input
The program takes 2 arguments:
- Path to input file
- Path to output file

```
> bin/spark-submit lshrec.py input.txt output.txt
```

Each line of the input file represents a user (with ID “U1” for example) and a list of movies the user has watched (e.g., movie \#0, 12, 45)

```
U2:U9,U10
U3:U8
U8:U3
…
```

*Assumption*: Each user has watched at least one movie.

# Output
For each user, with whom the program could find similar users, the output contains their top-5 similar users as follows:
```
U2:U9,U10
U3:U8
U8:U3
…
```
