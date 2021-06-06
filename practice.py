from greg_pkg_release.elkhelper import elkhelper


# elk = elkhelper('http://yourserver:19200',('username','password'))
elk=elkhelper()

table=[]
workday1 ={
    'id':1,'date':'2021-06-07'
}
workday2 ={
    'id':2,'date':'2021-06-08'
}
table.append(workday1)
table.append(workday2)


index='workdaytest'
# res = elk.searchAll('{}/_search?size=1'.format(index))


workday3 ={
    'id':3,'date':'2021-06-09'
}

res = elk.put(index,workday3)

ss=0



# res =elk.bulk(index,table)
# print(res)




















# res = elk.searchAll('workday/_search?size=2000')

# workdays =[]

# workday = {
#     'id':1
#     ,'time':'2021-06-06'
# }

# workdays.append(workday)

# workday2= {
#     'id':4
#     ,'time':'2021-06-17'
# }

# workdays.append(workday2)
# # res = elk.put('workdaytest',workday)

# res = elk.bulk('workdaytest',workdays)


# print(result)
# # res = elk.searchAll('workdaytest/_search?size=2000')


# ss=0

