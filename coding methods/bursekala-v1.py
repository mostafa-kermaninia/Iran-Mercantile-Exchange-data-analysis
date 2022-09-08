import asyncio
import aiohttp
import pymongo
import json
import datetime

#introduce mongo db 
mongoclient = pymongo.MongoClient("mongodb://localhost:27017/")
#data bases
updateFutureMarketsInfo = mongoclient['FutureMarketsInfo']  #1 آتي
updateMarketsInfo = mongoclient['MarketsInfo']              #2 اختيار
updateGavahiMarketsInfo = mongoclient['GavahiMarketsInfo']  #3 گواهي
updateSandoqMarketsInfo = mongoclient['SandoqMarketsInfo']  #4 صندوق
updateSalafMarketsInfo = mongoclient['SalafMarketsInfo']    #5 سلف
updateAllMarketData_updateFutureDateTime = mongoclient['AllMarketData_or_FutureDateTime']          #6,7 زمان و اطلاعات كلي





async def main() : 
    
    #introduce aiohttp client and sucket CND
    async with aiohttp.ClientSession() as session :
        link_address = 'wss://cdn.ime.co.ir/realTimeServer/connect?transport=webSockets&clientProtocol=2.1&connectionToken=\
            %2FVKQHAy6KEWn4ujDDNQSQ3sLvznVUzBULn5YG1z3bEhXtYqkRF8sMB%2BO1DcwdcWhz5ht7n8rzTJJhQAqyuCNbX1ziGJL50swBbgutZVTLRDm\
                5fnJsPsh6eZQ8dNirBsp&connectionData=%5B%7B%22name%22%3A%22marketshub%22%7D%5D&tid=4'
        ws = await session.ws_connect(link_address)    
        
        async for datum in ws : 
            
            if len(datum.data)>500 :
                print('lez goooooooooooooooo')
                print(datum)
                ####هر كدام از انواع ششگانه ي اپديت
                for update in datum.json()['M'] :
                    ####نام اپديت و كالكشن آن
                    update_type = update['M']
                                                                                                                                                                                             
                    ###آتي(1)
                    if update_type == 'updateFutureMarketsInfo' : 
                        
                        ##هر كدام از قرار داد ها
                        counter = 1
                        for contract in update['A'][0] :
                            
                            ##ساخت كالكشن براي هر قرارداد
                            col = updateFutureMarketsInfo[str(counter)]
                            counter += 1
                            contract['datetime'] = datetime.datetime.now()
                            col.insert_one(contract)        
                    
                    
                    ###اختيار (2)
                    elif update_type == 'updateMarketsInfo' : 
                        
                        ##هر كدام از قرار داد ها
                        counter = 1
                        for contract in update['A'][0] :
                            
                            ##ساخت كالكشن براي هر قرارداد
                            col = updateMarketsInfo[str(counter)]
                            counter += 1
                                
                            contract['datetime'] = datetime.datetime.now()
                            col.insert_one(contract)                      
                    
                                            
                    ###گواهي (3)
                    elif update_type == 'updateGavahiMarketsInfo' : 
                        
                        ##هر كدام از قرار داد ها
                        counter = 1
                        for contract in update['A'][0] :
                            
                            ##ساخت كالكشن براي هر قرارداد
                            col = updateGavahiMarketsInfo[str(counter)]
                            counter += 1
                                
                            contract['datetime'] = datetime.datetime.now()
                            col.insert_one(contract)    
                    
                                        
                    ###صندوق(4)
                    elif update_type == 'updateSandoqMarketsInfo' : 
                        
                        ##هر كدام از قرار داد ها
                        counter = 1
                        for contract in update['A'][0] :
                            
                            ##ساخت كالكشن براي هر قرارداد
                            col = updateSandoqMarketsInfo[str(counter)]
                            counter += 1
                                
                            contract['datetime'] = datetime.datetime.now()
                            col.insert_one(contract)                     
                    
                        
                    ###سلف(5)
                    elif update_type == 'updateSalafMarketsInfo' : 
                        
                        ##هر كدام از قرار داد ها
                        counter = 1
                        for contract in update['A'][0] :
                            
                            ##ساخت كالكشن براي هر قرارداد
                            col = updateSalafMarketsInfo[str(counter)]
                            counter += 1
                                
                            contract['datetime'] = datetime.datetime.now()
                            col.insert_one(contract) 
                
                            
                    ### اطلاعات كلي(6) 
                    elif update_type == 'updateAllMarketData' : 
                        info = update['A'][0]
                        
                        ## ساخت يك كالكشن براي اطلاعات كلي
                        col = updateAllMarketData_updateFutureDateTime['allmarketinformations']
                        info['datetime'] = datetime.datetime.now()
                        
                        # اضافه كردن كل داده ها در قالب يك ديكشنري   
                        col.insert_one(info)        
                    
                                    
                    ###(7)زمان 
                    elif update_type == 'updateFutureDateTime' : 
                        
                        ##زمان 
                        time = update['A'][0] 
                            
                        ##ساخت يك كالكشن براي كل اطلاعات
                        col = updateAllMarketData_updateFutureDateTime['timeinformations']
                                
                        # اضافه كردن زمان در يك ستون كالكشن   
                        col.insert_one({'key' : 'time' , 'value' : update['A'][0]})        
                                                    
                    ###خالي                                
                    elif update_type == '' :
                        None                            
                    
                    ###مشكل
                    else : 
                        print('noooooooooooooooooooooooooooooooooooooooo')
                    
                break                                
        
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(main())
        

