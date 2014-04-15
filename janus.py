################################################################################
#                                                                              #
# Janus                                                                        #
#                                                                              #
################################################################################
#                                                                              #
# LICENCE INFORMATION                                                          #
#                                                                              #
# Janus enables parallel processing in Python.                                 #
#                                                                              #
# copyright (C) 2013 2014 William Breaden Madden                               #
#                                                                              #
# This software is released under the terms of the GNU General Public License  #
# version 3 (GPLv3).                                                           #
#                                                                              #
# This program is free software: you can redistribute it and/or modify it      #
# under the terms of the GNU General Public License as published by the Free   #
# Software Foundation, either version 3 of the License, or (at your option)    #
# any later version.                                                           #
#                                                                              #
# This program is distributed in the hope that it will be useful, but WITHOUT  #
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or        #
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for    #
# more details.                                                                #
#                                                                              #
# For a copy of the GNU General Public License, see                            #
# <http://www.gnu.org/licenses/>.                                              #
#                                                                              #
################################################################################

import os
import os.path
import re
import signal
import sys
import tarfile
import time
import unittest
import uuid
import multiprocessing
import inspect
import base64
from datetime import datetime
import logging

# logging
logger = logging.getLogger(__name__)
logging.basicConfig()
logger.level = logging.DEBUG

## @brief configure janus
#  @detail This function allows configuration of janus.
#  @param loggerLevel logger level
def configure(
    loggerLevel = "INFO"
    ):
    if loggerLevel == "DEBUG":
        logger.level = logging.DEBUG
    elif loggerLevel == "INFO":
        logger.level = logging.INFO
    elif loggerLevel == "WARNING":
        logger.level = logging.WARNING
    elif loggerLevel == "ERROR":
        logger.level = logging.ERROR
    elif loggerLevel == "CRITICAL":
        logger.level = logging.CRITICAL
    else:
        exceptionMessage = "unknown logging level specified"
        logger.error("{notifier}: exception message: {exceptionMessage}".format(
            notifier = self.className,
            exceptionMessage = exceptionMessage
        ))
        raise Exception

## @brief returns a count of the number of processing cores
#  @detail This function returns a count of the number of processing cores.
def coreCount():
    return multiprocessing.cpu_count()

## @brief print in a human-readable way the items of a given object
#  @detail This function prints in a human-readable way the items of a given
#  object.
#  @param object to print
def printHR(object):
    # dictionary
    if isinstance(object, dict):
        for key, value in sorted(object.items()):
            print(u'{key}: {value}'.format(key = key, value = value))
    # list or tuple
    elif isinstance(object, list) or isinstance(object, tuple):
        for element in object:
            print(element)
    # other
    else:
        print(object)

## @brief return a URL-safe, base 64-encoded pseudorandom UUID
#  @detail This function returns a URL-safe, base 64-encoded pseudorandom
#  Universally Unique IDentifier (UUID).
#  @return string of URL-safe, base 64-encoded pseudorandom UUID
def uniqueIdentifier():
    return str(base64.urlsafe_b64encode(uuid.uuid4().bytes).strip("="))

## @brief return either singular or plural units as appropriate for a given
#  quantity
#  @detail This function returns either singular or plural units as appropriate
#  for a given quantity. So, a quantity of 1 would cause the return of singular
#  units and a quantity of 2 would cause the return of plural units.
#  @param quantity the numerical quantity
#  @param unitSingular the string for singular units
#  @param unitSingular the string for plural units
#  @return string of singular or plural units
def units(
    quantity = None,
    unitSingular = "unit",
    unitPlural = "units"
    ):
    if quantity == 1:
        return unitSingular
    else:
        return unitPlural

## @brief Job: a set of pieces of information relevant to a given work function
#  @detail A Job object is a set of pieces of information relevant to a given
#  work function. A Job object comprises a name, a work function, work function
#  arguments, the work function timeout specification, a
#  multiprocessing.Pool.apply_async() object and, ultimately, a result object.
#  @param name the Job object name
#  @param workFunction the work function object
#  @param workFunctionArguments the work function keyword arguments dictionary
#  @param workFunctionTimeout the work function timeout specification in seconds
class Job(object):

    ## @brief initialisation method
    def __init__(
        self,
        workFunction = None,
        workFunctionKeywordArguments = {},
        workFunctionTimeout = None,
        name = None,
        ):
        self.workFunction = workFunction
        self.workFunctionKeywordArguments = workFunctionKeywordArguments
        self.workFunctionTimeout = workFunctionTimeout
        self.className = self.__class__.__name__
        self.resultGetter = None
        if name == None:
            self._name = uniqueIdentifier()
        else:
            self._name = name
        if self.workFunction == None:
            exceptionMessage = "work function not specified"
            logger.error("{notifier}: exception message: {exceptionMessage}".format(
                notifier = self.className,
                exceptionMessage = exceptionMessage
            ))
            raise Exception

    @property
    def name(self):
        return self._name

    ## @brief return an object self description string
    #  @detail This method returns an object description string consisting of a
    #  listing of the items of the object self.
    #  @return object description string
    def __str__(self):
        descriptionString = ""
        for key, value in sorted(vars(self).items()):
            descriptionString += str("{key}:{value} ".format(
                key = key,
                value = value)
            )
        return descriptionString

    ## @brief print in a human-readable way the items of the object self
    #  @detail This function prints in a human-readable way the items of the
    #  object self.
    def printout(self):
        printHR(vars(self))

## @brief JobGroup: a set of Job objects and pieces of information relevant to a
#  given set of Job objects
#  @detail A JobGroup is a set of Job objects and pieces of information relevant
#  to a given set of Job objects. A JobGroup object comprises a name, a list of
#  Job objects, a timeout and, ultimately, an ordered list of result objects.
#  The timeout can be speecified or derived from the summation of the timeout
#  specifications of the set of Job objects.
#  @param name the JobGroup object name
#  @param jobs the list of Job objects
#  @param timeout the JobGroup object timeout specification in seconds
class JobGroup(object):

    ## @brief initialisation method
    def __init__(
        self,
        jobs = None,
        name = None,
        timeout = None
        ):
        self.jobs = jobs
        self.className = self.__class__.__name__
        self.completeStatus = False
        self.timeStampSubmission = None
        if name == None:
            self._name = uniqueIdentifier()
        else:
            self._name = name
        if timeout == None:
            self.timeout = 0
            for job in self.jobs:
                self.timeout += job.workFunctionTimeout
        self.results = []

    @property
    def name(self):
        return self._name

    ## @brief return an object self description string
    #  @ detail	This method returns an object description string consisting of
    #  a listing of the items of the object self.
    #  @return object description string
    def __str__(self):
        descriptionString = ""
        for key, value in sorted(vars(self).items()):
            descriptionString += str("{key}:{value} ".format(
                key = key,
                value = value)
            )
        return descriptionString

    ## @brief return Boolean JobGroup timeout status
    #  @detail This method returns the timeout status of a JobGroup object. If
    #  the JobGroup object has not timed out, the Boolean False is returned. If
    #  the JobGroup object has timed out, the Boolean True is returned. If the
    #  JobGroup object has been completed or is not submitted, the Boolean False
    #  is returned.
    #  @return Boolean indicating the JobGroup timeout status
    def timeoutStatus(self):
        # If the JobGroup is complete or not submitted, then it is not timed
        # out.
        if self.completeStatus is True or self.timeStampSubmission is None:
            return False
        # If the JobGroup is not complete or submitted, then it may be timed
        # out.
        elif time.time() > self.timeout + self.timeStampSubmission:
            return True
        else:
            return False

    ## @brief print in a human-readable way the items of the object self
    #  @detail This function prints in a human-readable way the items of the
    #  object self.
    def printout(self):
        printHR(vars(self))

## @brief initisation procedure for processes of process pool
def initialise_processes():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

## @brief ParallelJobProcessor: a multiple-process processor of Job objects
#  @param jobSubmission Job object or JobGroup object for submission
#  @param numberOfProcesses the number of processes in the process pool
class ParallelJobProcessor(object):

    ## @brief initialisation method that accepts submissions and starts pool
    #  @detail This method is the initialisation method of the parallel job
    #  processor. It accepts input JobGroup object submissions and prepares a
    #  pool of workers.
    def __init__(
        self,
        jobSubmission = None,
        numberOfProcesses = multiprocessing.cpu_count(),
        ):
        self.jobSubmission = jobSubmission
        self.numberOfProcesses = numberOfProcesses
        self.className = self.__class__.__name__
        self.status = "starting"
        logger.debug("{notifier}: status: {status}".format(
            notifier = self.className,
            status = self.status)
        )
        self.countOfJobs = None
        self.countOfRemainingJobs = 0
        self.pool = multiprocessing.Pool(
            self.numberOfProcesses,
            initialise_processes
        )
        logger.debug("{notifier}: pool of {numberOfProcesses} {units} created".format(
            notifier = self.className,
            numberOfProcesses = str(self.numberOfProcesses),
            units = units(quantity = self.numberOfProcesses,
            unitSingular = "process", unitPlural = "processes")
        ))
        self.status = "ready"
        logger.debug("{notifier}: status: {status}".format(
            notifier = self.className,
            status = self.status
        ))

    ## @brief return an object self-description string
    #  @detail This method returns an object description string consisting of
    #  a listing of the items of the object self.
    #  @return object description string
    def __str__(self):
        descriptionString = ""
        for key, value in sorted(vars(self).items()):
            descriptionString += str("{key}:{value} ".format(
                key = key,
                value = value
            ))
        return descriptionString

    ## @brief print in a human-readable way the items of the object self
    #  @detail This function prints in a human-readable way the items of the
    #  object self.
    def printout(self):
        printHR(vars(self)
        )

    ## @brief submit a Job object or a JobGroup object for processing
    #  @detail This method submits a specified Job object or JobGroup object
    #  for processing. On successful submission, it returns the value 0.
    #  @param jobSubmission Job object or JobGroup object for submission
    def submit(
        self,
        jobSubmission = None
        ):
        # If the input submission is not None, then update the jobSubmission
        # data attribute to that specified for this method.
        if jobSubmission != None:
            self.jobSubmission = jobSubmission
        self.status = "submitted"
        logger.debug("{notifier}: status: {status}".format(
            notifier = self.className,
            status = self.status
        ))
        # If the input submission is a Job object, contain it in a JobGroup
        # object.
        if isinstance(self.jobSubmission, Job):
            jobGroup = JobGroup(
                jobs = [self.jobSubmission,],
            )
            self.jobSubmission = jobGroup
        # Count the number of jobs.
        self.countOfJobs = len(self.jobSubmission.jobs)
        self.countOfRemainingJobs = self.countOfJobs
        # Build a contemporary list of the names of jobs.
        self.listOfNamesOfRemainingJobs = []
        for job in self.jobSubmission.jobs:
            self.listOfNamesOfRemainingJobs.append(job.name)
        logger.debug("{notifier}: received job group submission '{name}' of {countOfJobs} {units}".format(
            notifier = self.className,
            name = self.jobSubmission.name,
            countOfJobs = self.countOfJobs,
            units = units(
                quantity = self.countOfRemainingJobs,
                unitSingular = "job",
                unitPlural = "jobs"
            )
        ))
        logger.debug(self.statusReport())
        logger.debug("{notifier}: submitting job group submission '{name}' to pool".format(
            notifier = self.className,
            name = self.jobSubmission.name
        ))
        # Cycle through all jobs in the input submission and apply each to the
        # pool.
        for job in self.jobSubmission.jobs:
            job.timeStampSubmission = time.time()
            logger.debug("{notifier}: job '{name}' submitted to pool".format(
                notifier = self.className,
                name = job.name
            ))
            # Apply the job to the pool, applying the object pool.ApplyResult
            # to the job as a data attribute.
            job.resultGetter = self.pool.apply_async(
                func = job.workFunction,
                kwds = job.workFunctionKeywordArguments
            )
        # Prepare monitoring of job group times in order to detect a job group
        # timeout by recording the time of complete submission of the job group.
        self.jobSubmission.timeStampSubmission = time.time()
        logger.debug("{notifier}: job group submission complete: {countOfJobs} {units} submitted to pool (timestamp: {timeStampSubmission})".format(
            notifier = self.className,
            countOfJobs = self.countOfJobs,
            units = units(
                quantity = self.countOfJobs,
                unitSingular = "job",
                unitPlural = "jobs"
            ),
            timeStampSubmission = self.jobSubmission.timeStampSubmission
        ))
        self.status = "processing"
        logger.debug("{notifier}: status: {status}".format(
            notifier = self.className,
            status = self.status
        ))
        return 0

    ## @brief get results of JobGroup object submission
    #  @detail This method returns an ordered list of results for jobs
    #  submitted.
    #  @return order list of results for jobs
    def getResults(self):
        # While the number of jobs remaining is greater than zero, cycle over
        # all jobs in the JobGroup object submission submission, watching for a
        # timeout of the JobGroup object submission. If a result has not been
        # retrived for a job (i.e. the Job object does not have a result data
        # attribute), then check if a result is available for the job (using the
        # method multiprocessing.pool.AsyncResult.ready()). If a result is
        # available for the job, then check if the job has run successfully
        # (using the method multiprocessing.pool.AsyncResult.successful()). If
        # the job has not been successful, raise an exception, otherwise, get
        # the result of the job and save it to the result data attribute of the
        # job.
        logger.debug("{notifier}: checking for job {units}".format(
            notifier = self.className,
            units = units(
                quantity = self.countOfRemainingJobs,
                unitSingular = "result",
                unitPlural = "results")
            )
        )
        while self.countOfRemainingJobs > 0:
            # Check for timeout of the job group. If the current timestamp is
            # greater than the job group timeout (derived from the sum of the
            # set of all job timeout specifications in the job group) + the job
            # group submission timestamp, then raise an excepton, otherwise
            # cycle over all jobs.
            # Allow time for jobs to complete.
            time.sleep(0.25)
            if self.jobSubmission.timeoutStatus():
                logger.error("{notifier}: job group '{name}' timed out".format(
                    notifier = self.className,
                    name = self.jobSubmission.name
                ))
                self._abort()
                exceptionMessage = "timeout of a function in list {listOfNamesOfRemainingJobs}".format(
                    listOfNamesOfRemainingJobs = self.listOfNamesOfRemainingJobs
                )
                logger.error("{notifier}: exception message: {exceptionMessage}".format(
                    notifier = self.className,
                    exceptionMessage = exceptionMessage
                ))
                raise Exception
            else:
                for job in self.jobSubmission.jobs:
                    self.listOfNamesOfRemainingJobs = []
                    if not hasattr(job, 'result'):
                        # Maintain a contemporary list of the names of remaining
                        # jobs.
                        self.listOfNamesOfRemainingJobs.append(job.name)
                        # If the result of the job is ready...
                        if job.resultGetter.ready():
                            logger.debug(
                                "{notifier}: result ready for job '{name}'".format(
                                    notifier = self.className,
                                    name = job.name
                                )
                            )
                            job.successStatus = job.resultGetter.successful()
                            logger.debug(
                                "{notifier}: job '{name}' success status: {successStatus}".format(
                                    notifier = self.className,
                                    name = job.name,
                                    successStatus = job.successStatus
                                )
                            )
                            # If the job was successful, create the result data
                            # attribute of the job and save the result to it.
                            if job.successStatus:
                                job.result = job.resultGetter.get()
                                logger.debug(
                                    "{notifier}: result of job '{name}': {result}".format(
                                        notifier = self.className,
                                        name = job.name,
                                        result = job.result
                                    )
                                )
                                self.countOfRemainingJobs -= 1
                                logger.debug(
                                    "{notifier}: {countOfRemainingJobs} {units} remaining".format(
                                        notifier = self.className,
                                        countOfRemainingJobs = self.countOfRemainingJobs,
                                        units = units(
                                            quantity = self.countOfRemainingJobs,
                                            unitSingular = "job",
                                            unitPlural = "jobs"
                                        )
                                    )
                                )
                            # If the job was not successful, raise an exception
                            # and abort processing.
                            elif not job.successStatus:
                                logger.error(
                                    "{notifier}: job '{name}' failed".format(
                                        notifier = self.className,
                                        name = job.name
                                    )
                                )
                                self._abort()
                                exceptionMessage = "failure of function '{name}' with arguments {arguments}".format(
                                    name = job.workFunction.__name__,
                                    arguments = job.workFunctionKeywordArguments
                                )
                                logger.error("{notifier}: exception message: {exceptionMessage}".format(
                                    notifier = self.className,
                                    exceptionMessage = exceptionMessage
                                ))
                                raise Exception
        # All results having been returned, create the 'results' list data
        # attribute of the job group and append all individual job results to
        # it.
        self.jobSubmission.timeStampComplete = time.time()
        self.jobSubmission.completeStatus = True
        logger.debug("{notifier}: all {countOfJobs} {units} complete (timestamp: {timeStampComplete})".format(
            notifier = self.className,
            countOfJobs = self.countOfJobs,
            units = units(
                quantity = self.countOfJobs,
                unitSingular = "job",
                unitPlural = "jobs"
            ),
            timeStampComplete = self.jobSubmission.timeStampComplete
        ))
        self.jobSubmission.processingTime = self.jobSubmission.timeStampComplete - self.jobSubmission.timeStampSubmission
        logger.debug("{notifier}: time taken to process all {units}: {processingTime}".format(
            notifier = self.className,
            countOfJobs = self.countOfJobs,
            units = units(
                quantity = self.countOfJobs,
                unitSingular = "job",
                unitPlural = "jobs"
            ),
            processingTime = self.jobSubmission.processingTime
        ))
        for job in self.jobSubmission.jobs:
            self.jobSubmission.results.append(job.result)
            self._terminate()
        return self.jobSubmission.results
        self._terminate()

    ## @brief return a status report string
    #  @detail This method returns a status report string, detailing
    #  information on the JobGroup submission and on the job processing status.
    #  @return status report string
    def statusReport(
        self,
        type = "string"
        ):
        if type == "string":
            statusReport = "\n{notifier}:\n   status report:".format(
                notifier = self.className
            )
            # information on parallel job processor
            statusReport += "\n       parallel job processor configuration:"
            statusReport += "\n          status: {notifier}".format(
                notifier = str(self.status)
            )
            statusReport += "\n          number of processes: {notifier}".format(
                notifier = str(self.numberOfProcesses)
            )
            # information on job group submission
            statusReport += "\n       job group submission: '{notifier}'".format(
                notifier = self.jobSubmission.name
            )
            statusReport += "\n          total number of jobs: {notifier}".format(
                notifier = str(self.countOfJobs)
            )
            statusReport += "\n          number of incomplete jobs: {notifier}".format(
                notifier = str(self.countOfRemainingJobs)
            )
            statusReport += "\n          names of incomplete jobs: {notifier}".format(
                notifier = self.listOfNamesOfRemainingJobs
            )
            # information on jobs (if existent)
            if self.jobSubmission.jobs:
                statusReport += "\n       jobs:"
                for job in self.jobSubmission.jobs:
                    statusReport += "\n          job '{name}':".format(
                        name = job.name
                    )
                    statusReport += "\n              workFunction: '{name}'".format(
                        name = job.workFunction.__name__
                    )
                    statusReport += "\n              workFunctionKeywordArguments: '{arguments}'".format(
                        arguments = job.workFunctionKeywordArguments
                    )
                    statusReport += "\n              workFunctionTimeout: '{timeout}'".format(
                        timeout = job.workFunctionTimeout
                    )
                    if hasattr(job, 'result'):
                        statusReport += "\n              result: '{result}'".format(
                            result = job.result
                        )
            # statistics of parallel job processor run
            if hasattr(self.jobSubmission, 'processingTime'):
                statusReport += "\n       statistics:"
            if hasattr(self.jobSubmission, 'processingTime'):
                statusReport += "\n          total processing time: {processingTime} s".format(
                    processingTime = self.jobSubmission.processingTime
                )
            return statusReport
        elif type == "dictionary": #upcoming
            statusReport = {
                "status" : str(self.status),
                "number of processes" : str(self.numberOfProcesses),
                "job group name" : self.jobSubmission.name,
                "total number of jobs" : str(self.countOfJobs),
                "number of incomplete jobs" : str(self.countOfRemainingJobs),
                "names of remaining jobs" : self.listOfNamesOfRemainingJobs,
                "total processing time" : self.jobSubmission.processingTime
            }
            return statusReport

    ## @brief abort parallel job processor
    #  @detail This method aborts the parallel job processor. It is used
    #  typically when an exception is raised.
    def _abort(self):
        self.status = "aborting"
        logger.debug("{notifier}: status: {status}".format(
            notifier = self.className,
            status = self.status
        ))
        self._terminate()

    ## @brief terminate parallel job processor
    #  @detail This method terminates the parallel job processor. It terminates
    #  the subprocesses of the parallel job processor. It is used typically
    #  when terminating the parallel job processor on successful completion of
    #  job processing and when aborting the parallel job processor.
    def _terminate(self):
        self.status = "terminating"
        logger.debug("{notifier}: status: {status}".format(
            notifier = self.className,
            status = self.status
        ))
        logger.debug("{notifier}: terminating pool of {numberOfProcesses} {units}".format(
            notifier = self.className,
            numberOfProcesses = str(self.numberOfProcesses),
            units = units(
                quantity = self.numberOfProcesses,
                unitSingular = "process",
                unitPlural = "processes"
            )
        ))
        self.pool.terminate()
        self.pool.join()
        self.status = "finished"
        logger.debug("{notifier}: status: {status}".format(
            notifier = self.className,
            status = self.status
        ))
        logger.debug(self.statusReport())
