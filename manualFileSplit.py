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


class fileSplitBySize:
 
  def __init__(self,parmFileName,jobName,odate,splitDirectoryName=None,sourceFullFileName=None):
    print("initializing the constructor")
    self.parmFileName=parmFileName
    self.jobName=jobName
    self.odate=odate
    self.splitDirectoryName=splitDirectoryName
    self.sourceFullFileName=sourceFullFileName
    
    
  def split_cb(f, s):
    print("file: {0}, size: {1}".format(f, s))
      
  def file_split(self):
    print("getting the job specific details for the job {self.jobName}")
    print("calling the parm file parser module")
    parmDetails=parser.getParmDetails(self.jobName,self.parmFileName)
    print(parmDetails)
    pFileName=parmDetails.get('pfilename')
    pDate=parmDetails.get('pdate')
    pS3Bucket=parmDetails.get('ps3bucketname')
    pFilePath=parmDetails.get('pfilepath')
    pSplitSize=int(parmDetails.get('psplitsize',52428800))
    print("initializing the python FileSplit object")
    fs=Filesplit()
    self.splitDirectoryName=pFilePath+'split/'+pFileName.split('.',1)[0]+'/'
    self.sourceFullFileName=pFilePath+'raw/'+pFileName
    #check if the directory exists if not create it
    checkDirectory = os.path.isdir(self.splitDirectoryName)
    if not checkDirectory:
        #creating the directory
        logging.info("Directory %s not exists,creating the director",self.splitDirectoryName)
        os.makedirs(self.splitDirectoryName)
     
    fs.split(file=self.sourceFullFileName, split_size=pSplitSize, output_dir=self.splitDirectoryName,newline=True,callback=fileSplitBySize.split_cb)
    
    return True
    

class zipFiles:
  def __init__(self,splitLineCount=[],auditEntries=[],splitDirectoryName=None,sourceFullFileName=None):
    print("Initialing the constructor for ziping")
    self.splitDirectoryName=splitDirectoryName
    self.splitLineCount=splitLineCount
    self.auditEntries=auditEntries
    self.sourceFullFileName=sourceFullFileName
    
  def zipSplitFiles(self):
    files=os.listdir(self.splitDirectoryName)
    for fileName in files:
        print("aaaaaaa",fileName)
        if not fileName.startswith('fs'):
           full_path=os.path.join(self.splitDirectoryName,fileName)
           zipFileName = full_path+'.gz'
           with open(full_path,'rb') as f_input:
            with gzip.open(zipFileName,'wb') as f_output:
                shutil.copyfileobj(f_input,f_output)
                f_output.close()
            f_input.close()

  def zipFileLineCount(self):
    files=os.listdir(self.splitDirectoryName)
    for fileName in files:
        print("aaaaaaa",fileName)
        if not fileName.startswith('fs') and not fileName.endswith('.csv'):
           full_path=os.path.join(self.splitDirectoryName,fileName)
           print("full path inside the zipFileLine couny",full_path)
           grecmd="zgrep -Ec '$' "+ full_path
           totNoLines=subprocess.getoutput(grecmd)
           fileStats=int(check_output(["wc", "-l", full_path]).split()[0])
           print(totNoLines)
           self.splitLineCount.append(int(totNoLines))
    
  def auditValidation(self):
    #checking the source count and the file split count , if its same then continue the process
    srcLineCount=int(check_output(["wc", "-l", self.sourceFullFileName]).split()[0])
    splitLineTotCount=sum(self.splitLineCount)
    if srcLineCount != splitLineTotCount:
        print("audit failed for the total count and split file count",srcLineCount,splitLineTotCount)
        exit(1)

  def lineCount(self):
    files=os.listdir(self.splitDirectoryName)
    for fileName in files:
        print("aaaaaaa",fileName)
        if not fileName.startswith('fs'):
           full_path=os.path.join(self.splitDirectoryName,fileName)
           fileStats=int(check_output(["wc", "-l", full_path]).split()[0])
           print(fileStats)
           auditEntries.append(fileStats)
  

print("start of the execution")
 
if len(argv)!=4:
    print("number of paramteres didnt match")
    exit(1)


parmFileName=argv[1]
jobName=argv[2]
odate=argv[3]

print("paramters passed ",argv)
print("starting the file split")
flSplit=fileSplitBySize(parmFileName,jobName,odate,splitDirectoryName=None)
flSplit.file_split()
print(flSplit.splitDirectoryName,flSplit.sourceFullFileName)

print("creating the zipfile instance")
compress=zipFiles(splitDirectoryName=flSplit.splitDirectoryName,sourceFullFileName=flSplit.sourceFullFileName)
compress.zipSplitFiles()
compress.zipFileLineCount()
compress.auditValidation()
           


