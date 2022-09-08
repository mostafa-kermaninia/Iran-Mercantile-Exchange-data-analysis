import asyncio
import aiohttp
import pymongo
from cdn_specs import jalaly_datetime
import time
from urllib.parse import quote_plus
from random import randint
import re

# introduce mongo db
mongoclient = pymongo.MongoClient("mongodb://localhost:27017/")
## data base
cdn = mongoclient['cnd']
## collections
# (1) آتي
updateFutureMarketsInfo_col = cdn['FutureMarketsInfo']
# (2) اختيار
updateMarketsInfo_col = cdn['MarketsInfo']
# (3) گواهي
updateGavahiMarketsInfo_col = cdn['GavahiMarketsInfo']
# (4) صندوق
updateSandoqMarketsInfo_col = cdn['SandoqMarketsInfo']
# (5) سلف
updateSalafMarketsInfo_col = cdn['SalafMarketsInfo']
# (6) اطلاعات كلي
updateAllMarketData_col = cdn['AllMarketData']
# زمان (7)
updateFutureDateTime_col = cdn['FutureDateTime']
# (9) ارور ها
datalog = cdn['errors']

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
            print(msg)
            # filter little and useless data
            if len(msg.data) > 500 :
    
                # just for check
                print('lez goooooooooooooooo')
                # print(msg.data)

                # to partition every type of updates(totally 7 types)
                for update in msg.json()['M'] :

                    # name of update's type
                    update_type = update['M']

                    #### آتي (1)
                    if update_type == 'updateFutureMarketsInfo' :
   
                        # to partition contracts
                        for contract in update['A'][0] :
                            
                            # add codes
                            
                            # ati_comodities = ['سکه طلا' , 'زعفران' , 'نقره' , 'طلا' , 'پسته', 'زيره سبز', 'مس كاتد']
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
                            contract['full_name'] = contract['ContractDescription']                           
                            contract['datetime'] = jalaly_datetime                           

                            # insert all data in format of a dictionay
                            updateFutureMarketsInfo_col.insert_one(contract)


                    #### اختيار (2)
                    elif update_type == 'updateMarketsInfo' :
                        print('ekhhhhhhhhhhh')
                        # to partition contracts
                        for contract in update['A'][0] :
                            
                            # add codes
                            
                            # ekhtiar_comodities = ['سکه طلا' , 'زعفران']
                            if re.findall('زعفران' , contract['_CallContractDescription']) == ['زعفران'] :
                                contract['commodity'] = '1'
                            elif re.findall('سکه طلا' , contract['_CallContractDescription']) == ['سکه طلا'] :
                                contract['commodity'] = '3'        
                            else :
                                contract['commodity'] = '-1'                                                              
                                print('nooooooooooo')                                                                                
                            contract['update_type'] = '2'                           
                            contract['full_name'] = contract['_CallContractDescription']
                            contract['datetime'] = jalaly_datetime                           

                            # insert all data in format of a dictionay
                            updateMarketsInfo_col.insert_one(contract)


                    #### گواهي (3)
                    elif update_type == 'updateGavahiMarketsInfo' :

                        # to partition contracts
                        for contract in update['A'][0] :

                            # add codes
                            
                            # govahi_comodities = ['ميلگرد' , 'نخود' , 'زعفران' , 'نقره' , 'سيمان' , 'تمام سكه' , 'برنج' , 'كشمش' , 'طلا' , 'پسته', 'زيره سبز', 'مس كاتد']
                            if re.findall('زعفران' , contract['Name']) == ['زعفران'] :
                                contract['commodity'] = '1'                   
                            elif re.findall('نقره' , contract['Name']) == ['نقره'] :
                                contract['commodity'] = '4' 
                            elif re.findall('تمام سكه' , contract['Name']) == ['تمام سكه'] :
                                contract['commodity'] = '5' 
                            elif re.findall('برنج' , contract['Name']) == ['برنج'] :
                                contract['commodity'] = '6'                                                                    
                            elif re.findall('زيره سبز' , contract['Name']) == ['زيره سبز'] :
                                contract['commodity'] = '7'      
                            elif re.findall('كشمش' , contract['Name']) == ['كشمش'] :
                                contract['commodity'] = '8'                                                                                                   
                            elif re.findall('مس كاتد' , contract['Name']) == ['مس كاتد'] :
                                contract['commodity'] = '9'                                                                                                     
                            elif re.findall('ميلگرد' , contract['Name']) == ['ميلگرد'] :
                                contract['commodity'] = '10'    
                            elif re.findall('نخود' , contract['Name']) == ['نخود'] :
                                contract['commodity'] = '11'                                                            
                            elif re.findall('پسته' , contract['Name']) == ['پسته'] :
                                contract['commodity'] = '12' 
                            elif re.findall('سيمان' , contract['Name']) == ['سيمان'] :
                                contract['commodity'] = '13'                                                                                                  
                            else :
                                contract['commodity'] = '-1'                                                             
                                                                                                                 
                            contract['update_type'] = '3'                           
                            contract['full_name'] = contract['Name']                           
                            contract['datetime'] = jalaly_datetime   

                            # insert all data in format of a dictionay
                            updateGavahiMarketsInfo_col.insert_one(contract)


                    #### صندوق (4)
                    elif update_type == 'updateSandoqMarketsInfo' :

                        # to partition contracts
                        for contract in update['A'][0] :

                            # add codes
                            
                            # sandogh_comodities = ['سکه طلا' , 'زعفران(طلاي سرخ)', 'طلا']
                            if re.findall('زعفران' , contract['Name']) == ['زعفران'] :
                                contract['commodity'] = '1'
                            elif re.findall('طلاي سرخ' , contract['Name']) == ['طلاي سرخ'] :
                                contract['commodity'] = '1'                                
                            elif re.findall('سکه طلا' , contract['Name']) == ['سکه طلا'] :
                                contract['commodity'] = '3'    
                            elif re.findall('پشتوانه طلا' , contract['Name']) == ['پشتوانه طلا'] :
                                contract['commodity'] = '2'                                
                            else :
                                contract['commodity'] = '-1'                                                              
                                                                                                                 
                            contract['update_type'] = '4'                           
                            contract['full_name'] = contract['Name']
                            contract['datetime'] = jalaly_datetime   

                            # insert all data in format of a dictionay
                            updateSandoqMarketsInfo_col.insert_one(contract)


                    #### سلف (5)
                    elif update_type == 'updateSalafMarketsInfo' :

                        # to partition contracts
                        for contract in update['A'][0]:

                            # add codes
                            
                            # salaf_comodities = ['ميلگرد' , 'پلي اتيلن', 'سنگ آهن']
                            if re.findall('سنگ آهن' , contract['Name']) == ['سنگ آهن'] :
                                contract['commodity'] = '14'
                            elif re.findall('پلي اتيلن' , contract['Name']) == ['پلي اتيلن'] :
                                contract['commodity'] = '15'                                
                            elif re.findall('ميلگرد' , contract['Name']) == ['ميلگرد'] :
                                contract['commodity'] = '10'                                    
                            else :
                                contract['commodity'] = '-1'                                                              
                                                                                                                 
                            contract['update_type'] = '5'                           
                            contract['full_name'] = contract['Name']
                            contract['datetime'] = jalaly_datetime   

                            # insert all data in format of a dictionay
                            updateSalafMarketsInfo_col.insert_one(contract)


                    # اطلاعات كلي (6)
                    elif update_type == 'updateAllMarketData' :

                        # to find general info
                        info = update['A'][0]

                        # adding datetime to dictionary
                        info['comodity'] = '0'
                        info['update_type'] = '6'                           
                        info['full_name'] = 'اطلاعات كلي'
                        info['datetime'] = jalaly_datetime   

                        # insert all data in format of a dictionay
                        updateAllMarketData_col.insert_one(info)


                    # (7) زمان
                    elif update_type == 'updateFutureDateTime' :

                        # to find time
                        time = update['A'][0]
                        time_dict = {'time' : time}
                        
                        # adding datetime to dictionary
                        time_dict['comodity'] = '0'
                        time_dict['update_type'] = '7'                           
                        time_dict['full_name'] = 'اطلاعات زماني'
                        time_dict['datetime'] = jalaly_datetime   

                        # insert time in format of a dictionay
                        updateFutureDateTime_col.insert_one(time_dict)


                    # خالي
                    elif update_type == '' :

                        # insert log
                        my_dict = {'the problem': 'unknown datum'}
                        my_dict['update_type'] = '-1'
                        my_dict['full_name'] = 'آپديت تايپ خالي'
                        my_dict['datetime'] = jalaly_datetime   

                        datalog.insert_one(my_dict)

                        # save the data in format of json
                        file_name = "problemmaker_data.json"
                        file = open(file_name, "a")
                        file.write(msg.data)
                        file.close()


                    # مشكل
                    else:

                        # insert log
                        my_dict = {'the problem': 'unknown problem'}
                        my_dict['update_type'] = '-1'
                        my_dict['full_name'] = 'مشكل ناشناخته'
                        my_dict['datetime'] = jalaly_datetime   

                        datalog.insert_one(my_dict)

                        # save the data in format of json
                        file_name = "problemmaker_data.json"
                        file = open(file_name, "a")
                        file.write(msg.data)
                        file.close()


                # break


            
async def main() :
    io_session = aiohttp.ClientSession()
    negot = await negotiate(io_session)
    await websocket(negot["ConnectionToken"] , io_session)
    await io_session.close()
    
loop = asyncio.get_event_loop()
loop.create_task(main())
loop.run_forever()
