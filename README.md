# Async Python Program for truedata real-time feed
A fast way of working with multiple instruements reattime datafeed and processing them.This program is an adapter for working with [Truedata websocket API](https://www.truedata.in/api). Get telegram notification for script execution.

This program have been tested to work with **500+ symbols** realtime datafeed processing without any issues.


## How to use it
Step 1. Clone this project on your local machine.

Step 2. install all required dependencies by running below command.
```
pip install -r requirements.txt
```

Step 3. open td_adapter.py file in editor and update all below given parameters.
```
DB_URL = "YOUR_DB_URL"
DEV_CHAT_ID='YOUR_TELEGRAM_CHAT_ID'
TELEGRAM_TOKEN='YOUR_BOT_TOKEN'
TD_USERNAME ="YOUR_TD_USERNAME"
TD_PASS = "YOUR_TD_PASSWORD"
TD_RT_PORT =8084
TD_HS_PORT = 8092
```
Step 4. Now, run the program by executing below commands.
```
python -W ignore td_adapter.py
```
That's it !! Seat relax and ensure that you have stable internet connection. Rest will be taken care by the program.

**Note:** Telegram message integration is optional. You can comment out method ```__sendNotification()``` where ever it has called out.
