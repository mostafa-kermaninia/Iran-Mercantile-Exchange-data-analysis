import asyncio
import aiohttp
import time
from urllib.parse import quote_plus
from random import randint
import pandas as pd  

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
                # print(msg.data)
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
                            location = 'F:\\برنامه هاي نوشته با پايتون\\aiohttp project\\cdn\\constants.xlsx'
                            sheet_name = 'Ati_constants'
                            #خواندن كل فايل اكسل
                            with pd.ExcelFile(location) as book :   
                                
                                
                                 
                            #     #انداختن يك قرارداد اوليه در اكسل
                            #     sheet = pd.read_excel(book , sheet_name)
                            #     my_dict = {}
                                  
                            #     for field in contract :
                            #         if contract[field] == None :
                            #             my_dict[field] = 'None'
                            #         else :
                            #             my_dict[field] = [contract[field]]
                            #     sheet.append(pd.DataFrame(my_dict)).to_excel(location , sheet_name , index = False)
                                
                                
                                
                                #تبديل  شيت آتي فايل خوانده شده به ديتافريم پانداس                             
                                sheet = pd.read_excel(book , sheet_name)
                                #تبديل فيلد هاي اكسل به ديكشنري 
                                excel_dict_key_list = sheet.columns
                                excel_dict_value_list = sheet.iloc[ 0 , : ].values
                                excel_dict = {excel_dict_key_list[i] : excel_dict_value_list[i] for i in range(len(excel_dict_key_list))}
                                #  مقايسه ي ديكشنري ثوابت و ديكشنري قرارداد جديد ،و ريختن اشتراك آنها در ديكشنري ثوابت نهايي
                                # و برگرداندن آنبه اكسل
                                finall_constants = {k:[excel_dict[k]] for k in excel_dict.keys() if [excel_dict[k]] == [contract[k]]}
                                print(sheet.drop(labels=[0],axis=0))
                                print(finall_constants)
                                sheet.append(pd.DataFrame(finall_constants)).to_excel(location , sheet_name , index = False)

                            
                                
                                
                            
                    
                                
                                

# ###########################################################################################################################

            
            
            
            
            
async def main() :
    io_session = aiohttp.ClientSession()
    negot = await negotiate(io_session)
    await websocket(negot["ConnectionToken"] , io_session)
    await io_session.close()
    
loop = asyncio.get_event_loop()
loop.create_task(main())
loop.run_forever()
