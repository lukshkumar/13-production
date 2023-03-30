import ftplib
import sys
import os

FTP_HOST = "ftp.nyse.com"
FTP_USER = ""
FTP_PASS = ""

# connect to the FTP server
ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
# force UTF-8 encoding
ftp.encoding = "utf-8"
ftp.login()
# list current files & directories
# ftp.dir()


def downloadFiles(path,destination):
    #path & destination are str of the form "/dir/folder/something/"
    #path should be the abs path to the root FOLDER of the file tree to download
    try:
        ftp.cwd(path)
        #clone path to destination
        os.chdir(destination)
        os.mkdir(destination[0:len(destination)-1]+path)
        print(destination[0:len(destination)-1]+path+" built")
    except OSError as error:
        print(error)
        #folder already exists at destination
        pass
    except ftplib.error_perm:
        #invalid entry (ensure input form: "/dir/folder/something/")
        print("error: could not change to "+path)
        sys.exit("ending session")

    #list children:
    filelist=ftp.nlst()
    
    for file in filelist:
        try:
            #this will check if file is folder:
            ftp.cwd(path+file+"/")
            #if so, explore it:
            downloadFiles(path+file+"/",destination)
        except ftplib.error_perm:
            #not a folder with accessible content
            #download & return
            os.chdir(destination[0:len(destination)-1]+path)
            #possibly need a permission exception catch:
            with open(file,"wb") as f:
                ftp.retrbinary("RETR "+file, f.write)
            print(file + " downloaded")
    return

source="/Reference Data Samples/"
dest="C:/Users/Luksh Kumar/NYSE-FTP-DATA/"
downloadFiles(source,dest)

ftp.quit()