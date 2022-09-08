import asyncio
import aiohttp
import pymongo
from cdn_specs import URL, jalaly_datetime

# introduce mongo db
mongoclient = pymongo.MongoClient("mongodb://localhost:27017/")

# data bases
# (1) آتي
updateFutureMarketsInfo_db = mongoclient['FutureMarketsInfo2']
# (2) اختيار
updateMarketsInfo_db = mongoclient['MarketsInfo2']
# (3) گواهي
updateGavahiMarketsInfo_db = mongoclient['GavahiMarketsInfo2']
# (4) صندوق
updateSandoqMarketsInfo_db = mongoclient['SandoqMarketsInfo2']
# (5) سلف
updateSalafMarketsInfo_db = mongoclient['SalafMarketsInfo2']
# (6) , (7) زمان و اطلاعات كلي
updateAllMarketData_updateFutureDateTime_db = mongoclient['AllMarketData_or_FutureDateTime2']
# (8) ارور ها
datalog = mongoclient['errors']


async def main():

    # introduce aiohttp client and sucket cdn
    async with aiohttp.ClientSession() as session:
        ws = await session.ws_connect(URL)

        async for msg in ws:

            if len(msg.data) > 500:

                # just for check
                print('lez goooooooooooooooo')
                print(msg.data)

                # to partition every type of updates(totally 6 types)
                for update in msg.json()['M']:

                    # name of update's type
                    update_type = update['M']

                    #### آتي (1)
                    if update_type == 'updateFutureMarketsInfo':

                        # to partition contracts
                        for contract in update['A'][0]:

                            # make a collection for every contract
                            col = updateFutureMarketsInfo_db[contract['ContractDescription']]

                            # adding datetime to dictionary
                            contract['datetime'] = jalaly_datetime

                            # insert all data in format of a dictionay
                            col.insert_one(contract)

                    #### اختيار (2)
                    elif update_type == 'updateMarketsInfo':

                        # to partition contracts
                        for contract in update['A'][0]:

                            # make a collection for every contract
                            col = updateMarketsInfo_db[contract['_CallContractDescription']]

                            # adding datetime to dictionary
                            contract['datetime'] = jalaly_datetime

                            # insert all data in format of a dictionay
                            col.insert_one(contract)

                    #### گواهي (3)
                    elif update_type == 'updateGavahiMarketsInfo':

                        # to partition contracts
                        for contract in update['A'][0]:

                            # make a collection for every contract
                            col = updateGavahiMarketsInfo_db[contract['Name']]

                            # adding datetime to dictionary
                            contract['datetime'] = jalaly_datetime

                            # insert all data in format of a dictionay
                            col.insert_one(contract)

                    #### صندوق (4)
                    elif update_type == 'updateSandoqMarketsInfo':

                        # to partition contracts
                        for contract in update['A'][0]:

                            # make a collection for every contract
                            col = updateSandoqMarketsInfo_db[contract['Name']]

                            # adding datetime to dictionary
                            contract['datetime'] = jalaly_datetime

                            # insert all data in format of a dictionay
                            col.insert_one(contract)

                    #### سلف (5)
                    elif update_type == 'updateSalafMarketsInfo':

                        # to partition contracts
                        for contract in update['A'][0]:

                            # make a collection for every contract
                            col = updateSalafMarketsInfo_db[contract['Name']]

                            # adding datetime to dictionary
                            contract['datetime'] = jalaly_datetime

                            # insert all data in format of a dictionay
                            col.insert_one(contract)

                    # اطلاعات كلي (6)
                    elif update_type == 'updateAllMarketData':

                        # to find general info
                        info = update['A'][0]

                        # make a collection for info
                        col = updateAllMarketData_updateFutureDateTime_db['allmarketinformations']

                        # adding datetime to dictionary
                        info['datetime'] = jalaly_datetime

                        # insert general info in format of a dictionay
                        col.insert_one(info)

                    # (7)زمان
                    elif update_type == 'updateFutureDateTime':

                        # to find time
                        time = update['A'][0]

                        # make a collection for time
                        col = updateAllMarketData_updateFutureDateTime_db['timeinformations']

                        # insert time in format of a dictionay
                        col.insert_one(
                            {'key': 'time', 'value': time})

                    # خالي
                    elif update_type == '':

                        # insert log
                        my_dict = {'the problem': 'unknown datum',
                                   'time': jalaly_datetime}
                        datalog.insert_one(my_dict)

                        # save the data in format of json
                        file_name = "problemmaker_data.json"
                        file = open(file_name, "a")
                        file.write(msg.data)
                        file.close()

                    # مشكل
                    else:

                        # insert log
                        my_dict = {'the problem': 'unknown error',
                                   'time': jalaly_datetime}
                        datalog.insert_one(my_dict)

                        # save the data in format of json
                        file_name = "problemmaker_data.json"
                        file = open(file_name, "a")
                        file.write(msg.data)
                        file.close()

                break

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(main())
