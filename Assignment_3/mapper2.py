#!/usr/bin/python3

import sys

# Order of values in each line in the input file
# The first line of the input file is the header
# Values in each line are separated by '_,_' 
Id = 0
Body = 1
Tags = 2
Title = 3
Score = 4
ParentId = 5
ViewCount = 6
ClosedDate = 7
PostTypeId = 8
AnswerCount = 9
OwnerUserId = 10
LastEditDate = 11
CommentCount = 12
CreationDate = 13
FavoriteCount = 14
LastActivityDate = 15
LastEditorUserId = 16
AcceptedAnswerId = 17
OwnerDisplayName = 18
CommunityOwnedDate = 19
LastEditorDisplayName = 20

IS_AN_ACCEPTED_ANSWER = 'is_an_accepted_answer'

# Read the input from the STDIN one line at a time
for line in sys.stdin:
    
    vals = line.split('_,_')
    
    # Skipping the first line of the input file which is the header
    if vals [Id] == 'Id': 
        continue
    
    # The record is for a question post
    if vals[PostTypeId] == '1':
        if vals[AcceptedAnswerId] != '-':
            print('{}\t{}'.format(vals[AcceptedAnswerId].strip(), IS_AN_ACCEPTED_ANSWER))
    
    # The record is for an answer post
    elif vals[PostTypeId] == '2':
        print('{}\t{}'.format(vals[Id],vals[Score]))
