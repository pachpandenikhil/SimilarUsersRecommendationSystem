from pyspark import SparkContext
import sys, itertools, operator

# Forms input characteristic matrix tuples (sparse matrix)
def get_characteristic_matrix(users):
	for user in users:
		movies = user.split(',')
		user = movies[0]
		for movie in movies[1:]:
			yield(int(movie), int(user[1:]) - 1)

# Returns the signature matrix 
def get_signature_matrix(min_hash_values, user_count):
	matrix = [[200 for x in range(user_count)] for y in range(20)]
	for min_hash in min_hash_values:
		row = min_hash[0][0]
		col = min_hash[0][1]
		value = min_hash[1]
		if value < matrix[row][col]:
			matrix[row][col] = value
	return matrix

# Computes min hash. Returns min hash values in the form of tuples.
def compute_minhash(iterator):
	for tuple in iterator:
		row = tuple[0]
		col = tuple[1]
		
		# computing min hash values for 20 different hash functions
		for i in range(0,20):
			ith_min_hash =  (3*row + 13*i) % 100
			yield((i,col), ith_min_hash)


# Returns a list of tuples of all possible combinations of the input list of items
# eg. 
#	input:  list = [1,2,3] , choose = 2
#	output: [(1, 2), (1, 3), (2, 3)] 
def get_combinations(list, choose):
	return [user for user in itertools.combinations(list, choose)]

# Returns pairs of users who have the same value of min hash signatures for all rows
def get_candidate_pairs(rows):
	band_matrix = list(rows)
	band_matrix_transpose = map(list, zip(*band_matrix))
	signature_map = {}
	user_idx = 0
	for row in band_matrix_transpose:
		user_signature = '-'.join(str(min_hash) for min_hash in row)
		if user_signature in signature_map:
			signature_map[user_signature].append(user_idx)
		else:
			signature_map[user_signature] = []
			signature_map[user_signature].append(user_idx)
		user_idx += 1
	
	# computing candidate pairs
	candidate_pairs = []
	for key, value in signature_map.iteritems():
		if len(value) > 1:
			candidate_pairs.extend(get_combinations(value, 2))
	
	return candidate_pairs

# Returns map of user id and the set of movies watched by the user
def get_user_movies():
	global characteristic_matrix
	user_movies = {}
	for tuple in characteristic_matrix:
		movie = tuple[0]
		user = tuple[1]
		if user in user_movies:
			user_movies[user].add(movie)
		else:
			movies = set()
			movies.add(movie)
			user_movies[user] = movies
	return user_movies
	
	
# Computes Jaccard Similarity for each candidate pair of users
def compute_jaccard_similarity(iterator):
	global user_movies
	for candidate_pair in iterator:
		user_1 = candidate_pair[0]
		user_2 = candidate_pair[1]
		set_user_1 = user_movies[user_1]
		set_user_2 = user_movies[user_2]
		
		intersection_count = len(set_user_1.intersection(set_user_2))
		union_count =  len(set_user_1.union(set_user_2))
		jaccard_similarity = float(intersection_count)/union_count
		yield(candidate_pair,jaccard_similarity)

# Returns top 5 similar users for each user.
# Similar users are sorted in increasing order of their IDs.
# If a user has less than 5 similar users than that many users are only returned.
def get_similar_users(candidate_user_pairs):
	similarity_map = {}
	for candidate_pair in candidate_user_pairs:
		user_1 = candidate_pair[0][0]
		user_2 = candidate_pair[0][1]
		jaccard_similarity = candidate_pair[1]
		
		# addding user_1's entry
		if user_1 in similarity_map:
			similarity_map[user_1][user_2] = jaccard_similarity
		else:
			similarity_map[user_1] = {user_2 : jaccard_similarity}
		
		# addding user_2's entry
		if user_2 in similarity_map:
			similarity_map[user_2][user_1] = jaccard_similarity
		else:
			similarity_map[user_2] = {user_1 : jaccard_similarity}
	
	# computing top 5 similar users for each
	similar_users_map = {}
	for user, user_similarity_map in similarity_map.iteritems():
		# sorting user similarity map by decreasing order of jaccard similar (map value) and if the values are same then by key
		similar_users = [v[0] for v in sorted(user_similarity_map.iteritems(), key=lambda(k, v): (-v, k))]
		top_5_similar_users = similar_users[0:5]
		top_5_similar_users.sort()
		similar_users_map[user] = top_5_similar_users
	
	return similar_users_map

# Returns the list of each output line as prescribed by the output format.
# Each output line is a string and can be written directly to the output file.
def generate_output(similar_users_map):
	output = []
	sorted_similarity_map = sorted(similar_users_map.items(), key=operator.itemgetter(0))
	for user_similarity_map in sorted_similarity_map:
		user = user_similarity_map[0]
		user_similar_users = user_similarity_map[1]
		line = []
		line.append('U' + str(user + 1))
		line.append(':')
		for similar_user in user_similar_users:
			line.append('U' + str(similar_user + 1))
			line.append(',')
		output_line = ''.join(x for x in line[:-1])
		output.append(output_line)
		
	return output

# Writes the output to the file line by line.
def writeOutput(output, filePath):
    with open(filePath, 'w') as file:
		for line in output:
			file.write(line + '\n')

# main execution
if __name__ == '__main__':
	
	# reading input file and output file
	input_file = sys.argv[1]
	output_file = sys.argv[2]
	
	# initializing SparkContext
	sc = SparkContext(appName="LSH")

	# creating RDD
	users_rdd = sc.textFile(input_file)
	
	# forming input characteristic matrix
	characteristic_matrix_rdd = users_rdd.mapPartitions(get_characteristic_matrix)
	characteristic_matrix = characteristic_matrix_rdd.collect()
	
	# computing min hash signatures
	min_hash_values_rdd = characteristic_matrix_rdd.mapPartitions(compute_minhash)
	signature_matrix = get_signature_matrix(min_hash_values_rdd.collect(), users_rdd.count())
	
	# performing LSH with number of bands = 5
	signature_matrix_rdd = sc.parallelize(signature_matrix, 5)
	candidate_pairs_rdd = signature_matrix_rdd.mapPartitions(get_candidate_pairs)
	
	# computing Jaccard similarities for candidate paris
	user_movies = get_user_movies()
	jaccard_similarities_rdd = candidate_pairs_rdd.distinct().mapPartitions(compute_jaccard_similarity)
	
	# computing top 5 similar users for each user
	similar_users = get_similar_users(jaccard_similarities_rdd.collect())
	
	# writing the output to file
	writeOutput(generate_output(similar_users), output_file)