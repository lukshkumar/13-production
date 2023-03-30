import datetime
from pytz import timezone
import os
import boto3

def main():

    print("The S3-Uploader Program is Runing.......")
    
    tz = timezone("EST")

    bucket_name ='0-source-principal-administrative'
    ACCESS_KEY='AKIAV6D5TVIDJV7UMVSK'
    SECRET_KEY='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'
    
    # S3 Code 
    s3 = boto3.resource('s3',  
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY)
    
    your_bucket = s3.Bucket(bucket_name)
    path_to_get_data = "../Quandl/Quandl-Data-Split-Applied/"
    path_to_upload_data = "0-master-output/01-by-date/0-snp-plus/01-minute-data-by-date/"
    
    data_uploaded_on_sunday = False
        
    while True:
        
        today_date = datetime.datetime.now(tz).date()
        # Only Upload data on every Sunday as we won't be fetching any data on Sunday either from Quandl or TDAmeritrade.
        # 0 Means Monday. 6 Means Sunday.
        if((today_date.weekday() == 6) and (not data_uploaded_on_sunday)):
            
            data_uploaded_on_sunday = True
            
            print("Uploading Data to S3: Date: ", today_date, " Time: ", datetime.datetime.now().time())
                
            entries = os.listdir(path_to_get_data)
            
            #Sort all files by date
            entries.sort(reverse = True)

            for entry in entries:
                print(path_to_get_data + entry)
                your_bucket.upload_file(path_to_get_data + entry, path_to_upload_data + entry)
                print("File Uploaded: ", entry)

            print("INFO: Data Uploaded to S3 Successfully!")
        
        elif((today_date.weekday() == 5) and (data_uploaded_on_sunday)):
            
            data_uploaded_on_sunday = False
            

if __name__ == '__main__': main()