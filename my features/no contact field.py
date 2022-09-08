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
                            location = 'D:\\پايتون\\aiohttp project\\cdn\\constants.xlsx'
                            sheet_name = 'Ati_constants'
                            df = pd.read_excel(location , sheet_name=sheet_name)   

                            if len(list(df.keys())) == 0 :
                                
                                #firt row
                                df.append(pd.DataFrame(contract , index = ['value'])).to_excel(location , sheet_name)
                                print('no cell in excel founded,so a new contract added to excel as base contract')
                                
                            else :
                                
                                #main part
                                constants = {} 
                                keys=df.keys()
                                keys=keys.drop('Unnamed: 0')
                                for key in keys :
                                    if contract[key] == None :
                                        contract[key] = 'None'
                                    df1_value = list(df[key])[0]
                                    if df1_value == contract[key] :
                                        constants[key] = contract[key]

                                final_df = pd.DataFrame(constants ,index=['value'])
                                final_df.to_excel(location,sheet_name=sheet_name)


                                

# ###########################################################################################################################

            
            
            
            
            
async def main() :
    io_session = aiohttp.ClientSession()
    negot = await negotiate(io_session)
    await websocket(negot["ConnectionToken"] , io_session)
    await io_session.close()
    
loop = asyncio.get_event_loop()
loop.create_task(main())
loop.run_forever()
