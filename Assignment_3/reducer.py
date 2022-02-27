#!/usr/bin/python3

import sys


def get_month(creation_date):
    month = creation_date.split('-', 2)[1]
    if month == 1:
        month = "January"
    elif month == 2:
        month = "February"
    elif month == 3:
        month = "March"
    elif month == 4:
        month = "April"
    elif month == 5:
        month = "May"
    elif month == 6:
        month = "June"
    elif month == 7:
        month = "July"
    elif month == 8:
        month = "August"
    elif month == 9:
        month = "September"
    elif month == 10:
        month = "October"
    elif month == 11:
        month = "November"
    elif month == 12:
        month = "December"
    return month


def get_tags(tags):
    tag_list = [
        x.replace('<', '', 1).replace('>', '', 1)
        for x in tags.split('><')
    ]
    return tag_list


tag_data = {}

for line in sys.stdin:
    creation_date, tags = line.split('\t', 1)
    month = get_month(creation_date)
    tag_list = get_tags(tags)
    for tag in tag_list:
        tag_data[month][tag]["count"] += 1
for month in tag_data:
    print(month)
    for tag in month:
        print('{}\t{}'.format(tag, tag["count"]))
