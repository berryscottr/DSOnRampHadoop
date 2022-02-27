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

# Read the input from the STDIN one line at a time
for line in sys.stdin:
    
    vals = line.split('\t')
    
    # Skip the first line of the input file which is the header
    if vals [Id] == 'Id':
        continue

    # Skip the posts where the 'OwnerUserId' is unwokne
    if vals [OwnerUserId]  == '-' or vals [OwnerUserId] == '-1':
        continue

    # Skip the posts that are of type question
    if vals [PostTypeId] == '1':
        continue
    
    # Emit a new key-value record where key is 'OwnerUserId' and value is 1
    print('{}\t{}'.format(vals [OwnerUserId],1))

