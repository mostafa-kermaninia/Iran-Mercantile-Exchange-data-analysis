from datetime import datetime as dt
now = dt.now()

def get_jajaly_datetime(gy = int(now.year) , gm = int(now.month), gd = int(now.day)):
    
    g_d_m = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
    
    if (gm > 2):
        gy2 = gy + 1
        
    else:
        gy2 = gy
        
    days = 355666 + (365 * gy) + ((gy2 + 3) // 4) - ((gy2 + 99) // 100) + ((gy2 + 399) // 400) + gd + g_d_m[gm - 1]
    jy = -1595 + (33 * (days // 12053))
    days %= 12053
    jy += 4 * (days // 1461)
    days %= 1461
    
    if (days > 365):
        jy += (days - 1) // 365
        days = (days - 1) % 365
        
    if (days < 186):
        jm = 1 + (days // 31)
        jd = 1 + (days % 31)
        
    else:
        jm = 7 + ((days - 186) // 30)
        jd = 1 + ((days - 186) % 30)
        
    final_dt = dt.strptime(f'{jy}/{jm}/{jd} {now.hour}:{now.minute}:{now.second}' , '%Y/%m/%d %H:%M:%S')
    
    return final_dt

jalaly_datetime = get_jajaly_datetime()

import pymongo
# introduce mongo db
mongoclient = pymongo.MongoClient("mongodb://localhost:27017/")
## data base
cdn = mongoclient['cnd']
## collections
# (1) آتي
my_col = cdn['confirmation']

