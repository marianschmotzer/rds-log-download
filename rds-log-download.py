#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import os
import boto3
from tqdm import tqdm
import threading
import datetime
import time


class Output(object):

    def __init__(self, debug=False, quiet=False):
        """
        Create a Output object to print console messages and a progress bar

        :param debug: Print debug messages.
        :param quiet: Don't be so verbose and show only errors.
        """
        self.debugging = debug
        self.bequiet = quiet
        self.tqdm = tqdm(disable=True)

    def debug(self, message):
        """
        Print a debug messages. If parameter debug was set to false or quiet
        was set to true, those messages will not be shown.
        """
        if not self.bequiet and self.debugging:
                self.tqdm.write(datetime.datetime.utcnow().isoformat() + " DEBUG: "  + message)

    def info(self, message):
        """
        Print a info messages. If parameter quiet was set to true, those
        message will not be shown.
        """
        if not self.bequiet:
                self.tqdm.write(datetime.datetime.utcnow().isoformat() + " INFO: "+  message)

    def error(self, message):
        """
        Print a error messages. Those message will regardless of what debug
        and quiet parameters was set.
        """
        if not self.bequiet:
            self.tqdm.write(datetime.datetime.utcnow().isoformat() + " ERROR: " + message)

    def pbar(self, total):
        """
        Show a progress bar where total is the amount of items which will
        be processed. The progress can be updated with the update() method.
        """
        if not self.bequiet and sys.stdout.isatty():
            self.tqdm = tqdm(total=total)
    def update(self, n):
        """
        Update the progress bar by say that n items have been processed since
        last call of update.
        """
        self.tqdm.update(n)

    def close(self):
        """
        Close the progress bar.
        """
        self.tqdm.close()

class streamFiles(object):

    def __init__(self,instance,targetdir,NumberOfLines,poolingInterval,lastFile,debug):
        """
        Create DiskBackup object which back up list of given reposietoris.
        existing database with database with a referenc in the mastercloud.

        :param targetdir: Top level dir to keep data
        :param instance: instance to copy files from
        :param FileLastWritten: copy only files newer then specified
        """
        self.targetdir = os.path.abspath(targetdir)
        self.instance = instance
        self.NumberOfLines = NumberOfLines
        self.lastFile = lastFile
        self.poolingInterval = poolingInterval
        self.output = Output(debug)
        self.client = boto3.client('rds')

    def copyLastChanges(self,instance,filenameToCopy,marker,outputFile):
        nextPortion=True
        while nextPortion:
            try:
                logFilePortion = self.client.download_db_log_file_portion(DBInstanceIdentifier=instance,
                        LogFileName=filenameToCopy,Marker=marker,NumberOfLines=self.NumberOfLines)
            except Exception as e:
                self.output.info("Download exception skipping:" + str(e))
                break

            try:
                outputFile.write(logFilePortion['LogFileData'])
            except Exception as e:
                self.output.info("Store exception skipping:" + str(e))
            nextPortion=logFilePortion['AdditionalDataPending']
            marker=logFilePortion['Marker']
            outputFile.flush()
        return marker

    def run(self):
        """Main method to execute all the things."""
        if self.lastFile is None:
            response = self.client.describe_db_log_files(DBInstanceIdentifier=self.instance,FileLastWritten=0)
            lastFile=response['DescribeDBLogFiles'][len(response['DescribeDBLogFiles'])-1]
            currentFile=response['DescribeDBLogFiles'][len(response['DescribeDBLogFiles'])-1]
        else:
            currentFile=self.lastFile
        destDir=self.targetdir + "/" + self.instance
        destFile=destDir + "/" + os.path.basename(currentFile['LogFileName'])
        f = open(destFile,'w')
        marker='0'
        self.output.info("Streaming file " + destDir + "/" + os.path.basename(destFile) )
        while True :
            """Wait pooling interval seconds"""
            time.sleep(self.poolingInterval)
            response = self.client.describe_db_log_files(DBInstanceIdentifier=self.instance,FileLastWritten=0)
            lastFile=response['DescribeDBLogFiles'][len(response['DescribeDBLogFiles'])-1]
            marker=self.copyLastChanges(instance=self.instance,filenameToCopy=currentFile['LogFileName'],marker=marker,outputFile=f)
            """check if source file haven't chaged"""
            if currentFile['LogFileName'] != lastFile['LogFileName']:
                f.close()
                currentFile=lastFile
                destFile=destDir + "/" + os.path.basename(currentFile['LogFileName'])
                self.output.info("Streaming file " + os.path.basename(destFile) )
                f = open(destFile,'w')
                marker='0'



class CopyFiles(object):

    def __init__(self,instance,targetdir,FileLastWritten,NumberOfLines,debug):
        """
        Create DiskBackup object which back up list of given reposietoris.
        existing database with database with a referenc in the mastercloud.

        :param targetdir: Top level dir to keep data
        :param instancesToCopy: list of instances to copy files from
        :param FileLastWritten: copy only files newer then specified
        """
        self.targetdir = os.path.abspath(targetdir)
        self.instance = instance
        self.FileLastWritten = FileLastWritten
        self.NumberOfLines = NumberOfLines
        self.output = Output(debug=debug)
        self.client=boto3.client('rds')

    def copyOneFile(self,instance,filenameToCopy,destDir,fileSize):
        nextPortion=True
        marker='0'
        destFile=destDir + "/" + os.path.basename(filenameToCopy)
        if os.path.exists(destFile):
            statinfo = os.stat(destFile)
            if statinfo.st_size == fileSize :
                self.output.info("File " + destFile + " exists with correct size skiping")
                return
            else:
                f = open(destFile,'w')
        else:
            f = open(destFile,'a+')
        self.output.info("Dumping file " + os.path.basename(filenameToCopy) )
        while nextPortion:
            try:
                logFilePortion = self.client.download_db_log_file_portion(DBInstanceIdentifier=instance,
                        LogFileName=filenameToCopy,Marker=marker,NumberOfLines=self.NumberOfLines)
            except Exception as e:
                self.output.info("Download exception skipping:" + str(e))
                break
            f.write(logFilePortion['LogFileData'])
            nextPortion=logFilePortion['AdditionalDataPending']
            marker=logFilePortion['Marker']
        f.close()

    def run(self):
        """Main method to execute all the things."""
        if not os.path.exists(self.targetdir):
            os.makedirs(self.targetdir)

        try:
            if not os.path.exists(self.targetdir + "/" + self.instance ):
                os.makedirs(self.targetdir + "/" + self.instance)
        except Exception as e:
            self.output.error(str(e))

        response = self.client.describe_db_log_files(DBInstanceIdentifier=self.instance,FileLastWritten=self.FileLastWritten)
        numberOfFiles=len(response['DescribeDBLogFiles'])
        self.output.info("Number of files" + str(numberOfFiles))
        i=0
        #download all files but not last one
        while i < numberOfFiles-1 :
            fileToCopy=response['DescribeDBLogFiles'][i]
            self.copyOneFile(instance=self.instance,filenameToCopy=fileToCopy['LogFileName'],
                    destDir=self.targetdir + "/" + self.instance,fileSize=fileToCopy['Size'])
            i=i+1
        return response['DescribeDBLogFiles'][numberOfFiles-1]


def workerThread(instance,args,semaphore):
        """
        Thread class

        """
        instance = instance
        targetdir=args['targetdir']
        poolingInterval=args['poolingInterval']
        maxLines=args['maxLines']
        fromtime=args['fromtime']
        debug=args['debug']

        """Main method to execute all the things."""
        output.info("Thread started " + instance )
        semaphore.acquire()
        lastFile=CopyFiles(instance,targetdir,fromtime,maxLines,debug).run()
        semaphore.release()
        streamFiles(instance,targetdir,maxLines,poolingInterval,lastFile,debug).run()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@',
        description="Copy logs from AWS RDS instance log to local filesystem",
        epilog="All parameters can be stored to a file in --param=value format, "
                + "one parameter per line, then specify this file with @file.")

    parser.add_argument("-i", "--instance", metavar="List of instances",
                        dest="instancesToCopy",action="append",
                        help="List of instances to copy logs from." \
                              + "Can be specified multiple times. " )
    parser.add_argument("-a","--allinstances",
                        dest="allinstances",action="store_true",default=False,
                        help="Detect instances automaticly." )
    parser.add_argument("-e","--enginefilter", metavar="Filter this engines",
                        dest="enginefilter",default=None,
                        help="Filter only this engines when autodetect i on" )
    parser.add_argument("-t", "--targetdir", metavar="Target DIRECTORY", dest="targetdir",default='/tmp/',
                        help="Target top level directory ")
    parser.add_argument("--maxthreads", metavar="Target DIRECTORY", dest="maxThreads",type=int,default=3,
                        help="Maximum number of concurrent requests")
    parser.add_argument("-p", "--pooling", metavar="pooling interval",type=int,dest="poolingInterval",default=60,
                        help="AWS Pooling interval for streaming")
    parser.add_argument("-l", "--lines", metavar="LINES",type=int,dest="maxLines",default=1000,
                        help="Number of lines to get per request - larger numbers are better perf but may cause wierd behavour")
    parser.add_argument("--fromtime", metavar="LINES",type=int,dest="fromtime",default=0,
                        help="Download only files created after date format POSIX timestamp format")
    parser.add_argument("-d","--debug", action="store_true",dest="debug",default=False,
                        help="Set level to debug " )

    args = vars(parser.parse_args())
    output = Output(debug=args['debug'])
    threads = []
    """ we need to limit number of concurrent requests when downloading files otherwise AWS will break connection"""
    semaphore=threading.BoundedSemaphore(args['maxThreads'])
    """ Check if instances should be found automaticly or are specified"""
    if args['allinstances']:
        client=boto3.client('rds')
        listOfAllDatabases = client.describe_db_instances()
        listOfInstances=[]
        for i in listOfAllDatabases['DBInstances']:
            if args['enginefilter']== None:
                listOfInstances.append(i['DBInstanceIdentifier'])
            elif args['enginefilter']==i['Engine']:
                listOfInstances.append(i['DBInstanceIdentifier'])
            #print(i['DBInstanceIdentifier'])
    else:
        listOfInstances=args['instancesToCopy']

    for instance in listOfInstances:
        try:
            output.info("Starting thread for " + instance )
            t = threading.Thread(target=workerThread,args=(instance,args,semaphore))
            threads.append(t)
            t.start()
        except:
           output.error("Error: unable to start thread")
        #workerThread(instance,args)
    for t in threads:
        t.join()

