#!/usr/bin/python3

import sys

# Auxiliary variable to keep track of boundaries for records of a particular key (that is a user)
last_key = None
# Auxiliary variable to keep the sum of answers associated with a key 
last_key_values_sum = 0 

# Read the the intermediate key-value records from the STDIN one record (line) at a time
for line in sys.stdin:
    
    # Split the line to extract the key and value parts of the record
    key,value = line.split('\t',1)
    
    # Compare the ket with the last key we have read so far to see if 
    # all the records associated with th previous key are read and so a new key is being read
    if last_key != key:
        
        # If we are done reading the records associated with the previous key,
        # write the  
        if last_key:
            print('{}\t{}'.format(last_key,last_key_values_sum)) 
        
        # Reset the auxiliary variables 
        last_key = key
        last_key_values_sum = 0 
        
    # Add the count of the answers to the auxiliary variable keeping the sum of answers     
    last_key_values_sum += int(value)


# Wrie the number of the answers the last user has posted
if last_key:
    print('{}\t{}'.format(last_key,last_key_values_sum))
