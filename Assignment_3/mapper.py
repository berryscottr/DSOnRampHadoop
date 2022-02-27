#!/usr/bin/python3

import sys

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

for line in sys.stdin:
    vals = line.split('\t')
    if vals[Id] == 'Id':
        continue
    print('{}\t{}'.format(vals[CreationDate], vals[Tags]))
