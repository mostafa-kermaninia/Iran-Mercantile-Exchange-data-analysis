import asyncio
import aiohttp
import pymongo
from cdn_specs import jalaly_datetime
import time
from urllib.parse import quote_plus
from random import randint
import re
import pandas as pd  

# introduce mongo db
mongoclient = pymongo.MongoClient("mongodb://localhost:27017/")
## data base
cdn = mongoclient['cnd']
## collections
# (1) آتي
updateFutureMarketsInfo_col = cdn['FutureMarketsInfo']
update_confirmation = cdn['confirmation']



# socket the url
async def negotiate(io_session : aiohttp.ClientSession) :
    url = "https://cdn.ime.co.ir/realTimeServer/negotiate?clientProtocol=2.1&" \
          "connectionData=%5B%7B%22name%22%3A%22marketshub%22%7D%5D&_=" + str(int(time.time() * 1000))
    nego = await io_session.get(url)
    return await nego.json()
async def websocket(connection_token , io_session : aiohttp.ClientSession) :
    url = "wss://cdn.ime.co.ir/realTimeServer/connect?transport=webSockets&clientProtocol=2.1&connectionToken=" + \
          quote_plus(connection_token) + "&connectionData=%5B%7B%22name%22%3A%22marketshub%22%7D%5D&tid=" + \
          str(randint(1, 10))
    # start the crawling
    async with io_session.ws_connect(url) as ws :
        async for msg in ws :
           
            # filter little and useless data
            if len(msg.data) > 500 :    
                # just for check
                print('lez goooooooooooooooo')
                # to partition every type of updates(totally 7 types)
                for update in msg.json()['M'] :
                    # name of update's type
                    update_type = update['M']
                    #### آتي (1)
                    if update_type == 'updateFutureMarketsInfo' :
                        # to partition contracts
                        for contract in update['A'][0] :
                            
                            
                            
                            
                            
                            #ساعت 10 تا 12:30
###########################################################################################################################
#فرض شروع كد،داشتن اكسلي با يك داده ي الكي با كد 0 و يك كالكشن كاملا خاليست

                            location = 'D:\\پايتون\\aiohttp project\\\cdn\\c_n.xlsx'
                            sheet_name = 'آتي (update_type = 1)'
                            #خواندن كل فايل اكسل
                            with pd.ExcelFile(location) as book :    
                                #تبديل ستون اول و دوم شيت آتي فايل خوانده شده به ديتافريم پانداس                             
                                sheet = pd.read_excel(book , sheet_name , usecols = [0 , 1])
                                #ليست نام تمام قرارداد هاي موجود
                                full_names_list = sheet.iloc[ : , 0].values
                            
                                # ati_comodities = {'سکه طلا' , 'زعفران' , 'نقره' , 'طلا' , 'پسته', 'زيره سبز', 'مس كاتد'}
                                if re.findall('زعفران' , contract['ContractDescription']) == ['زعفران'] :
                                    contract['commodity'] = '1'
                                elif re.findall('طلا' , contract['ContractDescription']) == ['طلا'] :
                                    contract['commodity'] = '2'
                                elif re.findall('سكه' , contract['ContractDescription']) == ['سكه'] :
                                    contract['commodity'] = '3'                                   
                                elif re.findall('نقره' , contract['ContractDescription']) == ['نقره'] :
                                    contract['commodity'] = '4'    
                                elif re.findall('زيره سبز' , contract['ContractDescription']) == ['زيره سبز'] :
                                    contract['commodity'] = '7'                                                              
                                elif re.findall('مس كاتد' , contract['ContractDescription']) == ['مس كاتد'] :
                                    contract['commodity'] = '9'                                                              
                                elif re.findall('پسته' , contract['ContractDescription']) == ['پسته'] :
                                    contract['commodity'] = '12'                                                              
                                else :
                                    contract['commodity'] = '-1'                                                              
                                                                                                                     
                                contract['update_type'] = '1'                           
                                contract['datetime'] = jalaly_datetime  
                                                            
                                # قراردادي كه قبلا مشابه آن را داشته ايم               
                                if contract['ContractDescription'] in full_names_list : 
                                    
                                    # يافتن كد مربوطه و افزودن آن به ديكشنري قرارداد
                                    name_code = sheet.loc[sheet['full_name'] == contract['ContractDescription']]['name_code'].values[0].item()       
                                    contract['name_code'] = name_code
                                    
                                    #ساخت يك كوئري براي پيدا كردن قرارداد هايي با نام يكسان با قرارداد گرفته شده    
                                    query = {'name_code' : name_code}
                                    filter = {'_id' : 0 , 'LastUpdate': 0 , 'datetime' : 0 , 'PersianOrdersDateTime' : 0 }
                                    # جداكردن اخرين قراردادي با اين نام كه در ديتابيس ثبت شده
                                    last_doc_filtered = updateFutureMarketsInfo_col.find(query , filter).sort('datetime', -1).limit(1)
                                    #مجبوريم آنرا به ليست تبديل كنيم چون در حالت عادي نياز به ساخت حلقه براي يافت ديكشنري هاي درونش است و ما فقط يك ديكشنري درون اين داريم و نياز به حلقه نيست
                                    last_document = list(last_doc_filtered)
                                    #ليست ما فقط يك عضو دارد كه آن هم همان آخرين داكيومنت موجود با مشخصات دلخواه ما در كالكشن است  
                                    ## استفاده شد try اگر اكسل حذف نشود اما كالكشن را پاك كنيم ارور ميگيريم پس از دستور  :
                                    try :
                                        last_con = last_document[0]  
                                    except IndexError :
                                        last_con = []  

                                    # ساخت يك كپي از قرارداد تازه رسيده، و حذف كردن فيلد زمان كه هميشه متغير هست (چون دليلي بر آمدن ديتاي جديد نيست)
                                    new_con = contract.copy()
                                    new_con.pop('PersianOrdersDateTime')
                                    new_con.pop('LastUpdate')
                                    new_con.pop('datetime')

                                    # قرارداد با داده هاي جديد    
                                    if last_con != new_con :
                                        print('nooooooooooo')
                                        print(last_con)
                                        print(new_con)
                                        updateFutureMarketsInfo_col.insert_one(contract)
        
                                    #قرارداد با داده هاي تكراري
                                    elif last_con == new_con :
                                        print('yessssssssssssssssss')
                                        confirmation = {'commodity' : contract['commodity'] , 'update_type' : 1 , 'datetime' : jalaly_datetime , 'name_code' : f'{name_code} repetitive'}
                                        updateFutureMarketsInfo_col.insert_one(confirmation)

                                # قراردادي كه قبلا مشابهش را نداشتيه ايم
                                elif contract['ContractDescription'] not in full_names_list :
                                    print('ppppppppppppppppppppppp')
                                    # برداشتن كد اخرين داده ي اكسل  و ساختن كدقرارداد جديد برحسب آن
                                    last_name_code = sheet.tail(1).iloc[0 , 1].item()  
                                    name_code = last_name_code + 1
                                    
                                    #افزودن نام و كد اختصاصي كالاي جديد،به اكسل 
                                    sheet.append(pd.DataFrame({'full_name' : contract['ContractDescription'] , 'name_code' : [last_name_code + 1]})).to_excel(location , sheet_name , index = False)
                                    
                                    #افزودن كد كالاي جديد به ديكشنري قرارداد
                                    contract['name_code'] = name_code
                                    
                                    # insert all data in format of a dictionay
                                    updateFutureMarketsInfo_col.insert_one(contract)
                                                
                                
                           
                                
                      
                                    
                              
                                    
                                   
                                

###########################################################################################################################

            
            
            
            
            
async def main() :
    io_session = aiohttp.ClientSession()
    negot = await negotiate(io_session)
    await websocket(negot["ConnectionToken"] , io_session)
    await io_session.close()
    
loop = asyncio.get_event_loop()
loop.create_task(main())
loop.run_forever()
