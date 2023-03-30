import os
import boto3

def main():
    
    bucket_name ='ftp-data-nyse'
    ACCESS_KEY='AKIAV6D5TVIDJV7UMVSK'
    SECRET_KEY='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'

    # S3 Code 
    s3 = boto3.resource('s3',  
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    
    your_bucket = s3.Bucket(bucket_name)

    ftp_data_path = "C:\\Users\\Administrator\\NYSE-FTP-DATA\\" # Path to the FTP Data

    #count = 0
    folder_number = 0
    for root,dirs,files in os.walk(ftp_data_path):
        ftp_root = root.split("NYSE-FTP-DATA\\")[1]
        print("Folder Number:", folder_number)
        for file in files:
            s3_filename = os.path.join(ftp_root,file).replace("\\","/")
            your_bucket.upload_file(os.path.join(root,file),s3_filename)
        folder_number += 1
        # if(count==2):
        #     break
        # count += 1
        
    print("INFO: Data Uploaded to S3 Successfully!")

if __name__ == '__main__': 
    main()