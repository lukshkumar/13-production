import os
import boto3

def main():
    
    bucket_name ='0-source-principal-administrative'
    ACCESS_KEY='AKIAV6D5TVIDJV7UMVSK'
    SECRET_KEY='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'

    # S3 Code 
    s3 = boto3.resource('s3',  
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    
    your_bucket = s3.Bucket(bucket_name)
    path_to_get_data = "WinSCP-Processed/"
    path_to_upload_data = "0-master-output/01-by-date/1-snp-vix-options/"
    entries = os.listdir(path_to_get_data)
    
    #Sort all files by date
    entries.sort(reverse = True)

    for entry in entries:
        print(path_to_get_data + entry)
        your_bucket.upload_file(path_to_get_data + entry, path_to_upload_data + entry)
        print("File Uploaded: ", entry)

    print("INFO: Data Uploaded to S3 Successfully!")
    

if __name__ == '__main__': main()