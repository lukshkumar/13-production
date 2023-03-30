from datetime import datetime
import datetime as dt
import os
import sys
import pytz
sys.path.insert(1, '../Tickers/')
sys.path.insert(2, '../../Email/')
import EmailService

current_datetime = datetime.now(pytz.timezone("EST"))
current_date = current_datetime.date().strftime("%Y%m%d")
filename = f"{current_date}.csv"
cnn_filepath = f"../CNN/data/{current_date}.csv"
stocktwits_sentiments_filepath = f"../Stocktwits/data/sentiments/{current_date}.csv"
stocktwits_posts_filepath = f"../Stocktwits/data/posts/{current_date}.csv"


# CNN Alert
if(not os.path.exists(cnn_filepath)):

    Subject = "Failover Alert: CNN Fear and Greed"
    
    Body = "<h1 style='color:red'><u><center>Failover Alert - CNN Fear and Greed<center></u></h1>"
    
    Body += f"<h3>The program has failed to receive data for: {current_date} </h2>"
    
    Body += "<i><br><br>Thank you for reading the alert.</i>"

    EmailService.SendEmail(Subject, Body)

# Stocktwits Sentiments Alert
if(not os.path.exists(stocktwits_sentiments_filepath)):

    Subject = "Failover Alert: Stocktwits Sentiments"
    
    Body = "<h1 style='color:red'><u><center>Failover Alert - Stocktwits Sentiments<center></u></h1>"
    
    Body += f"<h3>The program has failed to receive data for: {current_date} </h2>"
    
    Body += "<i><br><br>Thank you for reading the alert.</i>"

    EmailService.SendEmail(Subject, Body)

# Stocktwits Posts Alert
if(not os.path.exists(stocktwits_posts_filepath)):

    Subject = "Failover Alert: Stocktwits Posts"
    
    Body = "<h1 style='color:red'><u><center>Failover Alert - Stocktwits Posts<center></u></h1>"
    
    Body += f"<h3>The program has failed to receive data for: {current_date} </h2>"
    
    Body += "<i><br><br>Thank you for reading the alert.</i>"

    EmailService.SendEmail(Subject, Body)

# WinSCP Options
date_for_winscp_options = current_datetime.date() - dt.timedelta(days=1)
#Only check for weekdays as we don't get Options data from WinSCP on weekends.
if(date_for_winscp_options.weekday() < 5): # 5 -Sat, 6 - Sun
    date_for_winscp_options = date_for_winscp_options.strftime("%Y%m%d")
    winscp_options_filepath = f"../WinSCP/Data/Extracted-Data/{date_for_winscp_options}.csv"
    # Stocktwits Posts Alert
    if(not os.path.exists(winscp_options_filepath)):

        Subject = "Failover Alert: Options Data from SFTP WinSCP"
        
        Body = "<h1 style='color:red'><u><center>Failover Alert - Options Data from SFTP WinSCP<center></u></h1>"
        
        Body += f"<h3>The program has failed to receive data for: {date_for_winscp_options} </h2>"
        
        Body += "<i><br><br>Thank you for reading the alert.</i>"

        EmailService.SendEmail(Subject, Body)

# Trading Economics Alert
trading_economics_month = current_datetime.date().month
trading_economics_year = current_datetime.date().year
date_for_trading_economics = f"{trading_economics_month}-{trading_economics_year}"

trading_economics_filepath = f"../Trading-Economics/Data/{date_for_trading_economics}.csv"
# Stocktwits Posts Alert
if(not os.path.exists(trading_economics_filepath)):

    Subject = "Failover Alert: Trading Economics Data"
    
    Body = "<h1 style='color:red'><u><center>Failover Alert - Trading Economics Data<center></u></h1>"
    
    Body += f"<h3>The program has failed to receive data for the month of: {date_for_trading_economics} </h2>"
    
    Body += "<i><br><br>Thank you for reading the alert.</i>"

    EmailService.SendEmail(Subject, Body)

