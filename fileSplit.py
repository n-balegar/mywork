import parmFileParser as parser
from sys import argv
from fsplit.filesplit import Filesplit
import logging
import gzip
import shutil
import os
import ntpath
from subprocess import check_output
import time
from zipfile import ZipFile
import subprocess

#print(argv)
#print(len(argv))
if len(argv)!=4:
    print("number of paramteres didnt match")
    exit(1)


parmFileName=argv[1]
jobName=argv[2]
odate=argv[3]
srcLineCount=0
#logging.basicConfig(filename='/mnt/d/movieLense/jobName.log', encoding='utf-8', level=logging.DEBUG)
logging.basicConfig(handlers=[logging.FileHandler(filename="/mnt/d/movieLense/log_records.txt", 
                                                 encoding='utf-8', mode='a+')],
                    format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                    datefmt="%F %A %T", 
                    level=logging.INFO)
#print(jobName)
parmDetails=parser.getParmDetails(jobName,parmFileName)
print(parmDetails)
fs=Filesplit()
splitLineCount=[]
auditEntries=[]

try:
    pFileName=parmDetails.get('pfilename')
    pDate=parmDetails.get('pdate')
    pS3Bucket=parmDetails.get('ps3bucketname')
    pFilePath=parmDetails.get('pfilepath')
    pSplitSize=int(parmDetails.get('psplitsize',52428800))

except:
    logging.debug("Paramters are not correct,please check the parmfile entries")
    exit(1)
logging.info("Paramteter passed are FilePath=%s,FileName=%s,Odate=%s,S3BucketName=%s split size=%s",pFilePath,pFileName,pDate,pS3Bucket,pSplitSize)

splitDirectoryName=pFilePath+'split/'+pFileName.split('.',1)[0]+'/'
sourceFullFileName=pFilePath+'raw/'+pFileName

def file_split():
    print(splitDirectoryName,sourceFullFileName)
    #open the file to get the total number of lines
    #check if the directory exists if not create it
    checkDirectory = os.path.isdir(splitDirectoryName)
    if not checkDirectory:
        #creating the directory
        logging.info("Directory %s not exists,creating the director",splitDirectoryName)
        os.makedirs(splitDirectoryName)

    fs.split(file=sourceFullFileName, split_size=pSplitSize, output_dir=splitDirectoryName,newline=True)


def auditValidation():
    #checking the source count and the file split count , if its same then continue the process
    srcLineCount=int(check_output(["wc", "-l", sourceFullFileName]).split()[0])
    splitLineTotCount=sum(splitLineCount)
    if srcLineCount != splitLineTotCount:
        print("audit failed for the total count and split file count",srcLineCount,splitLineTotCount)
        exit(1)

def lineCount():
    files=os.listdir(splitDirectoryName)
    for fileName in files:
        print("aaaaaaa",fileName)
        if not fileName.startswith('fs'):
           full_path=os.path.join(splitDirectoryName,fileName)
           fileStats=int(check_output(["wc", "-l", full_path]).split()[0])
           print(fileStats)
           auditEntries.append(fileStats)
     #return int(check_output(["wc", "-l", fileName]).split()[0])

def zipSplitFiles():
    files=os.listdir(splitDirectoryName)
    for fileName in files:
        print("aaaaaaa",fileName)
        if not fileName.startswith('fs'):
           full_path=os.path.join(splitDirectoryName,fileName)
           zipFileName = full_path+'.gz'
           with open(full_path,'rb') as f_input:
            with gzip.open(zipFileName,'wb') as f_output:
                shutil.copyfileobj(f_input,f_output)
                f_output.close()
            f_input.close()


def zipFileLineCount():
    files=os.listdir(splitDirectoryName)
    for fileName in files:
        print("aaaaaaa",fileName)
        if not fileName.startswith('fs') and not fileName.endswith('.csv'):
           full_path=os.path.join(splitDirectoryName,fileName)
           print("full path inside the zipFileLine couny",full_path)
           grecmd="zgrep -Ec '$' "+ full_path
           totNoLines=subprocess.getoutput(grecmd)
           fileStats=int(check_output(["wc", "-l", full_path]).split()[0])
           print(totNoLines)
           splitLineCount.append(int(totNoLines))

file_split()
zipSplitFiles()
zipFileLineCount()
#lineCount()
auditValidation()

