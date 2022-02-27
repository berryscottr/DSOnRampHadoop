#!/usr/bin/python3

import sys

# Auxiliary variable to keep sum of answer count for all posts 
answer_count_sum = 0  

# Auxiliary variable to keep the number of posts (there is an intermediate record for each post)
post_count = 0

# Read the the intermediate key-valye records from the STDIN one record (line) at a time
for line in sys.stdin:

    post_count += 1
    answer_count_sum += int(line.split('\t')[1])
    
# Compute the average number of answers per post    
average_answer_count = float(answer_count_sum)/post_count

# Print the result to the STDOUT
print('average_answer_count\t{}'.format(average_answer_count))
    
