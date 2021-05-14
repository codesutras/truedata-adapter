import logging
import telegram
import asyncio
import datetime
import time
import pandas as pd
import traceback as trace
from dateutil.relativedelta import relativedelta
from truedata_ws.websocket.TD import TD
from copy import deepcopy

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)
#URL for Hard logout
#https://api.truedata.in/logoutRequest?user=www75474&password=test6464&port=8084

DB_URL = "YOUR_DB_URL"
DEV_CHAT_ID='YOUR_TELEGRAM_CHAT_ID'
TELEGRAM_TOKEN='YOUR_BOT_TOKEN'
TD_USERNAME ="YOUR_TD_USERNAME"
TD_PASS = "YOUR_TD_PASSWORD"
TD_RT_PORT =8084
TD_HS_PORT = 8092

class trueDataListener:
    def __init__(self,api_call_type="HISTORY",db_conn=""):
        self.username = TD_USERNAME 
        self.password = TD_PASS  
        self.live_port = TD_RT_PORT
        self.history_port = TD_HS_PORT
        self.start_connection_time = datetime.datetime.strptime("06:31:05", "%H:%M:%S").time()
        self.end_connection_time = datetime.datetime.strptime("15:30:50", "%H:%M:%S").time()
        self.__api_call = api_call_type # Set HISTORY to get historical data and set LIVE for live streaming
        self.__db_conn = db_conn # db connection
        self.__instruments = ["NIFTY 50","NIFTY BANK","NIFTY20102911000CE","MCXCOMPDEX","HDFCBANK","ICICIBANK","RELIANCE","SBIN","TCS","WIPRO","YESBANK","ZEEL","NIFTY20OCTFUT", "NIFTY-I","BANKNIFTY-I","TCS20OCTFUT","RELIANCE20OCTFUT","UPL-I","NIFTY-I","BANKNIFTY-I","ALUMINIUM-I","NATURALGAS-I","RUBBER-I","COTTON-I","CRUDEOIL-I","SILVERMIC-I","GOLDM-I","SILVERM-I","COPPER-I", "SILVER-I"]

    def startProgramm(self):
        print('Starting programm')
        asyncio.get_event_loop().run_until_complete(self.initDataFeeder())
        return

    async def initDataFeeder(self):
        logger.info("*****BACKGROUND TASK: DATAFEEDER*****")
        logger.info("Initializing DataFeeder Program...")
        try:
            ###
            self.__TD_APP = TD(self.username, self.password, live_port=self.live_port, historical_port=None)
            if(self.__api_call == "HISTORY"):
                #call history Method
                await self.__updateHistory()
            else: #LIVE
                #call realtime method
                logger.info("Starting Live datafeed update")
                await self.__updateLive()
        except KeyboardInterrupt as e:
            logger.error("Program intrupted due to KeyboardInterrupt")
            await self.__close()
            curr_time = datetime.datetime.now()
            curr_time = curr_time.strftime("%d/%m/%Y %H:%M:%S")
            logger.debug("Datetime @ Program Closed :"+curr_time)
            #logger.error(trace.print_exc())
        except Exception as e:
            logger.error("Exception occured while running datafeed")
            logger.error(e)
            #logger.error(trace.print_exc())
        finally:
            content = "CONECTION <strong>CLOSE</strong> FOR UPDATING NFO INDEX DATA !!!"
            await self.__sendNotification(content)
            logger.debug('Program finished. Terminating script')
            await self.__close()
        return


    async def __close(self):
        logger.debug('Closing Program...')
        self.__TD_APP.stop_live_data(self.__instruments)
        logger.info("Stopped the Datafeed subscription")
        self.__TD_APP.disconnect()
        asyncio.get_event_loop().stop()


    async def __updateHistory():
        logger.info("Coming soon.")
        pass


    # @abstract This method is to update historical data for given symbol
    # @version 1.0.0
    # @author JM
    async def __updateLive(self):
        try:
            logger.info("Starting realtime feed")
            content = "PROGRAMM STARTED FOR UPDATING DATA !!!"
            await self.__sendNotification(content)
            td_app = self.__TD_APP
            #Below symbol list need to by dynamic so that it can store multiple symbols according
            symbols = self.__instruments
            req_ids = td_app.start_live_data(symbols)
            live_data_objs = {}
            time.sleep(1)
            for req_id in req_ids:
                live_data_objs[req_id] = deepcopy(td_app.live_data[req_id])
                logger.debug("touchlinedata:")
                logger.debug(td_app.touchline_data[req_id])

            while (datetime.datetime.now().time() > self.start_connection_time and datetime.datetime.now().time() < self.end_connection_time):
                for req_id in req_ids:
                    if not td_app.live_data[req_id] == live_data_objs[req_id]:
                        logger.debug("Tick information:")
                        logger.debug(td_app.live_data[req_id])
                        if(td_app.live_data[req_id].timestamp is not None and td_app.live_data[req_id].ltq is not None):
                            data = {
                                    "timestamp":td_app.live_data[req_id].timestamp,
                                    "symbol":td_app.live_data[req_id].symbol,
                                    "ltp":td_app.live_data[req_id].ltp,
                                    "ltq": td_app.live_data[req_id].ltq,
                                    "oi":td_app.live_data[req_id].oi
                                    }

                            #Call function to save data
                            #await self.__saveLiveData(data)
                            live_data_objs[req_id] = deepcopy(td_app.live_data[req_id])

                        else:
                            time_t1 = datetime.datetime.now()
                            time_t1 = time_t1.strftime("%d/%m/%Y %H:%M:%S")
                            logger.debug("Empty timestamp and LTP in tick:"+time_t1)
                    else:
                        # time_t2 = datetime.datetime.now()
                        # time_t2 = time_t2.strftime("%d/%m/%Y %H:%M:%S")
                        # logger.debug("No Data received :"+time_t2)
                        pass
            #Code to stop connection
            td_app.stop_live_data(symbols)
            logger.info("Stopped the Datafeed subscription")
            td_app.disconnect()
            curr_time = datetime.datetime.now()
            curr_time = curr_time.strftime("%d/%m/%Y %H:%M:%S")
            logger.info("Datetime @ Program Closed :"+curr_time)
            logger.info("End of Market Hours")
            # content = "CONECTION <strong>CLOSE</strong> FOR UPDATING NFO INDEX DATA !!!"
            # await self.__sendNotification(content)
            # logger.debug('Program finished. Terminating script')

        except (KeyboardInterrupt,EOFError) as e:
            logger.error("Program intrupted due to KeyboardInterrupt")
            curr_time = datetime.datetime.now()
            curr_time = curr_time.strftime("%d/%m/%Y %H:%M:%S")
            logger.debug("Datetime @ Program Closed :"+curr_time)
            logger.error(e)
            #logger.error(trace.print_exc())
        return True

    # @abstract This method is to save data into db
    # @version 1.0.0
    # @author JM
    async def __saveLiveData(self,data):
        try:


            logger.info("Adding data into Artic db")
            logger.info(data)
            df = await self.__parsing_json(data)
            symbol = data['symbol']
            df['symbol'] = symbol
            '''
	    Write logic to save data
	    NOTE: self.__parsing_json() method written a pandas dataframe
	    '''
        except Exception as e:
            logger.error("Exception Occured while saving")
            logger.error(e)
            #logger.error(trace.print_exc())



    # @abstract This method will send a telegram NOTIFICATION
    # @author JM
    # @version 1.0.0
    async def __sendNotification(self,content,parse_mode=None):
        logger.debug("Calling telegram API..")
        try:
            bot = telegram.Bot(TELEGRAM_TOKEN)
            #print("Message to be notified.",data)
            message = "<strong>NOTIFICATION !!</strong> \n"
            message += content
            bot.send_message(chat_id=DEV_CHAT_ID,text=message,parse_mode=telegram.ParseMode.HTML)
        except Exception as e:
            logger.error("Exception occured while sending telegram message")
            logger.error(e)
            #logger.error(trace.print_exc())


    # @abstract This method is written to parse the data received from websocket
    # @author JM
    # @version 1.0.0
    # @# NOTE:  This meothod will parse received data from realtime call
    async def __parsing_json(self,data):

        try:
            timestamps = [data['timestamp']]
            unix_timestamp = [data['timestamp'].timestamp()]
            dates = [data['timestamp']]
            opens = [data['ltp']]
            closes = [data['ltp']]
            highs = [data['ltp']]
            lows = [data['ltp']]
            openinterest = [data['oi']]
            volumes = [data['ltq']]

            df_dict = {'index':timestamps,'unix_timestamp':unix_timestamp,
                       'date':dates,'open': opens, 'high': highs, 'low': lows,
                       'close': closes, 'volume': volumes,'openinterest':openinterest}
            df = pd.DataFrame(df_dict)
            df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df['index'] = pd.to_datetime(df['index'])
            df.set_index('index', inplace=True)
        except Exception as e:
            logger.error("Error while parsing data")
            logger.error(e)
            #logger.error(trace.print_exc())
        return df


# # # To run this as a standalone data downloader, uncomment the below code and run this python file.
if __name__ == "__main__":
    conn = ""#DBUtil(DB_URL)
    td_obj = trueDataListener(api_call_type="LIVE",db_conn=conn)
    td_obj.startProgramm()
