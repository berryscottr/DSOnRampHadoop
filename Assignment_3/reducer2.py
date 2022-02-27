#!/usr/bin/python3

import sys

# Auxiliary variable to keep sum of scores for accepted answers 
scores_sum = 0  

# Auxiliary variable to keep the number of accepted answers
accepted_answer_count = 0

# Auxiliary variable to keep track of boundaries for records of a particular key
pre_record_key = None

# Auxiliary variable to keep values associated with a key 
values = []

IS_AN_ACCEPTED_ANSWER = 'is_an_accepted_answer'

# Read the the intermediate key-value records from the STDIN one record (line) at a time
for line in sys.stdin:

    key,value = line.strip().split('\t')
    
    if key != pre_record_key:
        
        # A new key is arrived, so all the records associated with the previous key are read.
        # Now check if there are two intermediate records for the previous key
        # If yes, this indicates that the previous key was Id of an accepted answer 
        if len(values) == 2:
            
            accepted_answer_count += 1 
            index_of_score = 1-values.index(IS_AN_ACCEPTED_ANSWER)
            scores_sum += int(values[index_of_score])
                
        pre_record_key = key
        # Clear the auxiliary variable to store the values of the new key
        values = []
    
    # Store the value associated with the key
    values.append(value)
        
# Compute the average score of an accepted answer    
average_score = float(scores_sum)/accepted_answer_count

# Print the result to the STDOUT
print('average_accepted_answer_score\t{}'.format(average_score))
    
