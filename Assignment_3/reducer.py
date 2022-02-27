#!/usr/bin/python3

import sys


def get_month(creation_date):
    month_int = int(creation_date.split('-', 2)[1])
    if month_int == 1:
        month = "January"
    elif month_int == 2:
        month = "February"
    elif month_int == 3:
        month = "March"
    elif month_int == 4:
        month = "April"
    elif month_int == 5:
        month = "May"
    elif month_int == 6:
        month = "June"
    elif month_int == 7:
        month = "July"
    elif month_int == 8:
        month = "August"
    elif month_int == 9:
        month = "September"
    elif month_int == 10:
        month = "October"
    elif month_int == 11:
        month = "November"
    elif month_int == 12:
        month = "December"
    return month_int, month


def get_tags(tags):
    tag_list = [
        x.replace('<', '', 1).replace('>', '', 1).replace('\n', '', -1)
        for x in tags.split('><')
    ]
    tag_list_cleaned = []
    for tag in tag_list:
        if tag != "-":
            tag_list_cleaned.append(tag)
    return tag_list_cleaned


tag_data = {
    "January": {
        "Tags": {}
    },
    "February": {
        "Tags": {}
    },
    "March": {
        "Tags": {}
    },
    "April": {
        "Tags": {}
    },
    "May": {
        "Tags": {}
    },
    "June": {
        "Tags": {}
    },
    "July": {
        "Tags": {}
    },
    "August": {
        "Tags": {}
    },
    "September": {
        "Tags": {}
    },
    "October": {
        "Tags": {}
    },
    "November": {
        "Tags": {}
    },
    "December": {
        "Tags": {}
    }
}

for line in sys.stdin:
    creation_date, tags = line.split('\t', 1)
    month_int, month = get_month(creation_date)
    tag_list = get_tags(tags)
    for tag in tag_list:
        if tag in tag_data[month]["Tags"]:
            tag_data[month]["Tags"][tag] += 1
        else:
            tag_data[month]["Tags"][tag] = 1
for month in tag_data:
    print('====={}====='.format(month))
    for k, v in tag_data[month]["Tags"].items():
        print('{}\t{}'.format(k, v))
