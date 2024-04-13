# DARLING: Data-Aware Load Shedding in Complex EventProcessing Systems

We are sincerely honored and grateful for the opportunity to submit our paper
 to VLDB.  
Of course, if the paper will be accepted we will publish our code as open-source, publically available code (as an extension to openCEP https://github.com/ilya-kolchinsky/OpenCEP).
This submission directory contains three folders with implementation for each
 of DARLING, eSPICE, and hSPICE algorithms.  
All algorithms use left-deep-tree as their evaluation mechanism.
All algorithms are implemented in python3.8

## Background
Darling is a load shedding CEP mechanism that maintains a latency bound yet
 maximizing the system's score, which is represented by the number of output
  matches detected.

## Datasets
The following datasets were used during our evaluations:
- *Stock dataset*: This dataset was taken from historical records of the
 NASDAQ stock market: http://www.eoddata.com. 
    The data contains a one-year period of updates to stock prices, covering over 2500 stock identifiers with prices updated on a per-minute basis.
    Each record represents a primitive event, with the stock identifier used as the event type.
    Our input stream contained 409,622,891 events, each consisting of a stock identifier, a timestamp, the trading volume, and the prices at open, close, high, and low points.
    Since this data was bought in money, we could not supply it here.
    The dataset weighs 18G.
    
    **Since we bought this dataset, we did not attach it**.
    
    The following table demonstrates a sample of this dataset
    
    | Event type (stock symbol)| Timestamp  | Open       | High       | Low        | Close      | Volume     |
    | ------------ |:----------:|:----------:|:----------:|:----------:|:----------:|:----------:|
    | ERIC | 200802010900 | 31.55 | 31.55 | 31.55 | 31.55 | 20000 |
    | MSFT | 200802010900 | 41.32 | 41.32 | 41.25 | 41.25 | 199424 |
    | YHOO | 200802010900 | 38.89 | 38.9 | 38.85 | 38.86 | 69022 |

- *Soccer dataset*:  This dataset was taken from the DEBS 2013 Grand
 Challenge: https://debs.org/grand-challenges/2013/.
 The link for downloading the dataset is: http://www2.iis.fraunhofer.de/sports-analytics/full-game.gz (it weighs 4G).
The data originates from wireless sensors embedded in the soccer players' shoes and a ball used during a single match; the data spans the duration of the entire game. 
The sensors for the players produced data at a frequency of 200Hz, while the sensor in the ball produced data at a frequency of 2000Hz. 
Each row in the dataset contains a sensor identifier, which is used for the event type, along with the timestamp in pico-seconds, the location's x, y, z coordinates in mm, the velocity, and acceleration. 
This dataset contains 49,576,080 records (i.e., primitive events).

    **Due to its large size, we did not attach the complete dataset. 
    However, we added a sample file that can be found in experiments/datasets/soccer_sample.txt**.
    This sample file contains 500,000 events of types 19, 23, 47, 49, 53, 57
    , 59, 61 (sensor ids), so that you can test any pattern contains these event
     types.

    The following table demonstrates a sample of this dataset:
    
    | Event type (sensor ID)| Timestamp | x | y | z | v | a | vx | vy | vz | ax | ay | az |
    | ---------- |:----------:|:----------:|:----------:|:----------:|:----------:|:----------:|:----------:|:----------:|:----------:|:----------:|:----------:|:----------:|
    | 67 | 10634731737624712 | 26679 | -580 | 194 | 140437 | 607509 | 2074 | -855 | -9744 | 9661 |    2063 | -1549 |
    | 66 | 10634732514566388 | 27813 | -558 | 67 | 142069 | 1233916 | -5400 | -636 | 8392 | -158 | -7878 | 6156 |
    | 75 | 10634735140223609 | 26254 | -1097 | -154 | 82907 | 837089 | -539 | 9671 | 2485 | 7827 | 6169 | -816 |



- *Bus dataset*: This dataset contains bus GPS data collected from across
 Dublin by the Dublin City Council's traffic control.
  It can be downloaded from: https://data.smartdublin.ie/dataset/dublin-bus-gps-sample-data-from-dublin-city-council-insight-project.
  It weighs 3.5G.
  The data spans  a one-month period and covers more than 80 line IDs, which were used as event types. 
    This dataset contains 33,097,325 records and each record contains the timestamp, line ID, direction, journey pattern ID, vehicle journey ID, bus operator, latitude, longitude, delay, block ID, vehicle ID, and stop ID updated every few seconds or milliseconds.
    We augmented the data with an additional attribute named dist, which indicates the distance of the bus from the coordinates of Dublin's city center.

    The following table demonstrates a sample of this dataset
    
    | Event type (bus ID) | time | direction | journey_pattern_id | time_frame | vehicle_journey_id | operator | congestion | lon | lat | delay | block_id | vehicle_id | stop_id | at_stop | dist |
    |------------|:----------:|:----------:|:----------:|:----------:|:----------:|:----------:|:----------:|:----------:|:----------:|:----------:|:----------:|:----------:|:----------:|:----------:|:----------:|
    | 15 | 1352160000000000 | 0 | 50 | 0 | 5826 | 6 | 0 | -6.258584 | 53.340099 | -361 | 15013 | 33210 | 4870 | 0 | 1225.9839380522612 |
    | 66 | 1352160002000000 | 0 | 438 | 0 | 2940 | 5 | 0 | -6.372633 | 53.355251 | -362 | 66001 | 33588 | 3993 | 0 | 7113.026471985399 |
    | 145 | 1352160004000000 | 0 | 249 | 0 | 6815 | 3 | 0 | -6.2305160000000015 | 53.31783299999999 | -860 | 145101 | 36035 | 794 | 0 | 4308.670674530701 |
    
    **Due to its large size, we did not attach the complete dataset. 
    However, we added a sample file that can be found in darling/experiments/datasets/bus_sample.txt**.
    This sample file contains 500,000 events of types 13, 145, 15, 25, 27, 38
    , 39, 66 (line ids), so that you can test any pattern contains these event
     types.


## Architecture
All algorithms are composed of two containers:
1)  **Generator** : reads an input event stream from a file and send it to
 the processor container through a raw tcp socket in dynamic rates.
2) **Processor** : accepts input events from the generator and performs the load
 shedding process and the evaluation process.

## Dependencies
The preferred way for running the experiments (as we performed) is by running
 docker containers.  
 This ensures the same environment for each container and allows us
  to limit the processor container for simulating resource-constrained
   environment.
### For running using docker
- You should have Docker with version >= 20.10.5 installed.

### For running locally using python
- You should have python>=3.8 installed.
- You should have to insert custom environment variables for the experiments.  
  It is operating-system dependant and can be performed easily using pycharm
   IDE.


## Configure Experiments
For running your own experiment, you should change the environment variables
 file: env , or alternatively create a new environment file.
(If you run locally without docker, you should expose those environment
 variables).  
Each folder of an algorithm contains example environment variables file named
 "env".
 The common environment variables for all algorithms, are the following:
 - IP : the ip (ipv4, separated by dots) of the machine you run the docker
  containers on.
 - PORT : the port the processor will listen on (make sure the port is
  available for use)
 - DATASET_KIND : one of the three options: stock / bus / soccer for which
  the pattern belongs.
 - DATA_FILE : the filename of the dataset
 - DATASET_SIZE : the size of the dataset for process.
 - WARM_UP : how many events to process for gathering statistics in the warm
 -up phase (in the preprocessing phase)
 - SLEEP_LOAD : the number of seconds for the generator to add to the sleep
  time of events send as load
  event in the load scenario
 - SLEEP_REG : the number of seconds for the generator to sleep in regular mode
  (for not
  creating overload)
 - ARRIVAL_RATES : path to a file containing the number of seconds to sleep
  after sending each event.
  It should contain only float numbers separated by '\n'.
  It allows us to generate dynamic arrival rates, and control overload by
   using SLEEP_REG and SLEEP_LOAD environment variables.
 - MAX_LATENCY : the maximum number of seconds of the detection latency of a
  match, such
  that when exceeded, the match would not be calculated in the percentage of
   detected matches.
 -  BOOST: the percentage of the data to send as load, and the corresponding
  percentage of data after which we start to send as load.
  For example, the value 3:30,40:10,60:20,90:10 would mean: after sending 3
  % of the data as non overload (sleeping SLEEP_REG extra time between sending
  two consecutive events), send the next 30% of the data as
   load (sleeping SLEEP_LOAD extra time between consecutive events. Note that
    SLEEP_REG > SLEEP_LOAD). Then, after sending 40% of the data, send more
     10% as load, and after sending 60% of the data, send more 20% as load
     , etc.
 - PATTERN : the pattern you want to detect. For example, in stock dataset
    , one may want to detect the pattern:  
    SEQ(AAPL a, MSFT b, INTC c) WHERE a.volume > b.volume and b.volume > c
    .volume WITHIN 60 minutes
 - L_MAX : the maximum latency bound for the given experiment (in seconds).  
    (it is called LB in eSPICE, hSPICE, and ICDE'20 env files)
 
 
 Custom variables for DARLING:
 - BETA: the fraction of N_in after which we start dropping events.  
 - PSI : a list of values of psi_T for each event type. If only one value is
  passed, it will be the psi value for all event types.
  
 Custom variables for hSPICE and eSPICE:
  - WS : the average size of the window in the pattern
  - MAX_WINDOW_STATS: the number of windows to create the utility-table from

Custom variables for ICDE'20:
  - NUM_CLASSES: the number of classes of partial matches for data abstructions.
  - NUM_TIME_SLICES: the number of time slices for temportal abstructions.


## How to run the experiments
We will describe how to run an example experiment for the pattern:  
SEQ(61 a, 8 b, 13 c) WHERE |a.x - b.x| <= 50000 and c.v > a.v * 1.2 WITHIN
 15000000000 picoseconds  
which is the pattern Q1 from soccer queries in the paper (only on 50,000
 events and with smaller window size for the experiment to finish shortly).
## Run experiment using Docker
We will next describe how to run the example experiment with the
 configurations in env file of each of the algorithms.

For your convenience, we supplemented a script for running this easily.  
The script is named as 'run_experiments.sh'.
For running the script, you should supply you ip address (\<ip> , do not
 use localhost), and 4
 available ports (\<port1>, \<port2>, \<port3>, \<port4>).  
 To run the script, do the following:
 1. Grant it execution permissions `chmod +x run_experiments.sh`
 2. Run the script `./run_experiments.sh <ip> <port1> <port2> <port3> <port4>`  
    For example: ` ./run_experiments.sh 1.2.3.4 11000 12000 13000 14000`  
 
 The script generates two containers for each algorithm, a processor
  container and a generator container.  
  The naming conventions for the containers are: \<alg>-processor and \<alg>-generator where
  alg is darling, hspice, or espice.  
 You can see the containers logs using `docker logs <container-name>`.
   

### Run locally without docker
1. supply the environment variables
2. run processor.py
3. run generator.py

## Experiment results
Each experiment results contain the following fields:
- num_shedded: number of events shedded in DARLING, and number of times that
 event was dropped from window in espice, and the number of times event was
  dropped from partial match in hspice. 
- matches_latency: the average match latency measured in the experiment
, which is the metric we
 want to
 maintain
- storage_size: the peak memory usage in bytes
- storage_size_avg: the average memory usage in MB
- found_matches: the number of detected matches. For receiving the percentage
of detected matches we ran the experiment without load shedding to receive
 the absolute number of matches for this pattern.
- time: the time for running the experiment
- throughput: number of events processed per second 

Intermediate results such as the current number of events processed and
 number of detected matches, or the number of events already sent by the
  generator can be seen
 using logs: `docker logs <container-name>`.
 
 Feel free to contact us if more explanation is needed :)
