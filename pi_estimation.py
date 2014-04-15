import math
import time
import random
import janus as janus

def piMC(iterations = 100):
    count = 0
    for i in range(iterations):
        if math.pow(random.random(), 2.0) + math.pow(random.random(), 2.0) <= 1.0:
            count += 1
    pi_estimation = 4.0*count/iterations
    return pi_estimation

def piMCSerial(iterations = 100):
    print("\nserial estimation of pi...")
    timeStampStart = time.time()
    pi_estimation = piMC(iterations)
    timeStampStop = time.time()
    print("pi estimation: {piEstimation}".format(piEstimation = pi_estimation))
    print("processing time: {processingTime} s\n".format(processingTime = timeStampStop - timeStampStart))

def piMCParallel(iterations = 100):
    print("parallel estimation of pi...")
    # Create a group of jobs.
    jobs = []
    numberOfProcesses = janus.coreCount()
    jobTimeout = 180
    iterations = int(iterations/numberOfProcesses)
    for currentJob in xrange(0, numberOfProcesses):
        jobs.append(
            janus.Job(
                name = "estimation of pi",
                workFunction = piMC,
                workFunctionKeywordArguments = {'iterations': iterations},
                workFunctionTimeout = jobTimeout
            )
        )
    jobGroup1 = janus.JobGroup(
        name = "estimation of pi",
        jobs = jobs
    )
    # Prepare the parallel job processor.
    parallelJobProcessor1 = janus.ParallelJobProcessor()
    # Submit the jobs to the parallel job processor.
    parallelJobProcessor1.submit(jobSubmission = jobGroup1)
    resultsList = parallelJobProcessor1.getResults()
    #statusReport = parallelJobProcessor1.statusReport(type = "dictionary")
    #print("status report:")
    #print(statusReport)
    print("result produced by each CPU core: {resultsList}".format(resultsList = resultsList))
    pi_estimation = sum(resultsList)/numberOfProcesses
    print("pi estimation: {piEstimation}".format(piEstimation = pi_estimation))
    print("processing time: {processingTime} s\n".format(processingTime = statusReport["total processing time"]))

def main():
    #janus.configure(loggerLevel = "DEBUG")
    janus.configure(loggerLevel = "INFO")
    MCIterations = 100000000
    piMCSerial(iterations = MCIterations)
    piMCParallel(iterations = MCIterations)

if __name__ == '__main__':
    main()