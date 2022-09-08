from calendar import c
import json
from typing import Counter
import pymongo 
import json
import datetime
import asyncio
#introduce mongo db 
mongoclient = pymongo.MongoClient("mongodb://localhost:27017/")
#data bases
updateFutureMarketsInfo = mongoclient['FutureMarketsInfo']  #1 آتي
updateMarketsInfo = mongoclient['MarketsInfo']              #2 اختيار
updateGavahiMarketsInfo = mongoclient['GavahiMarketsInfo']  #3 گواهي
updateSandoqMarketsInfo = mongoclient['SandoqMarketsInfo']  #4 صندوق
updateSalafMarketsInfo = mongoclient['SalafMarketsInfo']    #5 سلف
updateAllMarketData_updateFutureDateTime = mongoclient['AllMarketData_or_FutureDateTime']          #6,7 زمان و اطلاعات كلي



with open('cnd_data(11.18)(14,30).json', 'r' , encoding='utf-8') as fp :
    
    #####كل اطلاعات موجود
    data = json.load(fp)['M']
    
    ####هر كدام از انواع ششگانه ي اپديت
    for update in data :
        
        ####نام اپديت و كالكشن آن
        data_type = update['M']
        
        
        ###آتي(1)
        if data_type == 'updateFutureMarketsInfo' : 
            
            ##هر كدام از قرار داد ها
            counter = 1
            for contract in update['A'][0] :
                
                ##ساخت كالكشن براي هر قرارداد
                col = updateFutureMarketsInfo[str(counter)]
                counter += 1
                contract['datetime'] = datetime.datetime.now()
                col.insert_one(contract)        
         
         
        ###اختيار (2)
        elif data_type == 'updateMarketsInfo' : 
            
            ##هر كدام از قرار داد ها
            counter = 1
            for contract in update['A'][0] :
                
                ##ساخت كالكشن براي هر قرارداد
                col = updateMarketsInfo[str(counter)]
                counter += 1
                    
                contract['datetime'] = datetime.datetime.now()
                col.insert_one(contract)                      
         
                                   
        ###گواهي (3)
        elif data_type == 'updateGavahiMarketsInfo' : 
            
            ##هر كدام از قرار داد ها
            counter = 1
            for contract in update['A'][0] :
                
                ##ساخت كالكشن براي هر قرارداد
                col = updateGavahiMarketsInfo[str(counter)]
                counter += 1
                    
                contract['datetime'] = datetime.datetime.now()
                col.insert_one(contract)    
        
                            
        ###صندوق(4)
        elif data_type == 'updateSandoqMarketsInfo' : 
            
            ##هر كدام از قرار داد ها
            counter = 1
            for contract in update['A'][0] :
                
                ##ساخت كالكشن براي هر قرارداد
                col = updateSandoqMarketsInfo[str(counter)]
                counter += 1
                    
                contract['datetime'] = datetime.datetime.now()
                col.insert_one(contract)                     
        
            
        ###سلف(5)
        elif data_type == 'updateSalafMarketsInfo' : 
            
            ##هر كدام از قرار داد ها
            counter = 1
            for contract in update['A'][0] :
                
                ##ساخت كالكشن براي هر قرارداد
                col = updateSalafMarketsInfo[str(counter)]
                counter += 1
                    
                contract['datetime'] = datetime.datetime.now()
                col.insert_one(contract) 
       
                
        ### اطلاعات كلي(6) 
        elif data_type == 'updateAllMarketData' : 
            info = update['A'][0]
            
            ## ساخت يك كالكشن براي اطلاعات كلي
            col = updateAllMarketData_updateFutureDateTime['allmarketinformations']
            info['datetime'] = datetime.datetime.now()
            
            # اضافه كردن كل داده ها در قالب يك ديكشنري   
            col.insert_one(info)        
         
                         
        ###(7)زمان 
        elif data_type == 'updateFutureDateTime' : 
            
            ##زمان 
            time = update['A'][0] 
                
            ##ساخت يك كالكشن براي كل اطلاعات
            col = updateAllMarketData_updateFutureDateTime['timeinformations']
                    
            # اضافه كردن زمان در يك ستون كالكشن   
            col.insert_one({'key' : 'time' , 'value' : update['A'][0]})        
                                        
        ###خالي                                
        elif data_type == '' :
            None                            
        
        ###مشكل
        else : 
            print('noooooooooooooooooooooooooooooooooooooooo')