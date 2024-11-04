# fmi.weather.finland


## Overview
This repository contains code to extract FMI meteorological data from CSC Allas into Puhti. The code can be run on Puhti by using an interactive R session or as a batch job. The data can be extracted for a set of coordinates or for the coordinates within a provided polygon. The data is for 1961-2023 and can be requested at three different spatial resolutions (1km-by-1km, 5km-by-5km and 9km-by-9km). The code extracts the data for the nearest neighbours of the requested coordinates.

## Instructions on use
### Setup
1. **Login to Puhti.**
2. **Start an interactive R session:**
    
    1. Proceed with steps 3-6 before using the R session.

### Load the Allas module
The Allas module must be loaded. This only needs to be done once for a project.

3. **Click the** ```Open in Terminal``` **button on the Puhti dashboard**.
4. **In the terminal type the following commands and follow the instructions:**

    1. ```module load allas ```
    
    2. ```allas-conf -m S3 ```
    
### Setup working directory
5. **Navigate to the working directory of your project or create a new one.**

6. **Set environment variables:**

    Some environment variables must be set before running. Perhaps the easiest way is by creating a     ```.Renviron``` file in the working directory from which the programme will be executed.
    
    Create an empty ```.Renviron``` like this:
    1. Open a terminal
    2. Navigate into your working dir: ```cd <your_working_dir>```
    3. Then type ```touch .Renviron```.
    4. Check that the file was created by typing ```ls -a```
    
    The file will not be visible when viewing the directory on the Puhti dashboard.

7. **Open the interactive R session:**

    1. In the Files Pane, click on the three dots (Go to directory) and paste in your directory. You should see          the ```.Renviron``` file in the files tab.
    2. Open the ```.Renviron``` file and paste in the contents from here:
    [.Renviron-contents](https://github.com/ForModLabUHel/fmi.weather.finland/blob/fmi-from-allas/docs/Renviron_example.txt).
    3. Create a new R script and paste in the contents of the example script from here:
       [Example-script](https://github.com/ForModLabUHel/fmi.weather.finland/blob/fmi-from-allas/scripts/get_fmi_from_allas.R).
    4. Run the command ```setwd(</your_working_dir>)```.
    5. Click on the ```Session``` tab in the top bar and select ```Terminate R...```.
    6. After R restarts you should see ```</your_working_dir>``` in the Files Pane and ```getwd()``` should              point to it too.
    7. Follow the instructions in the example script.
    
    
8. **Running as a batch job**

    1. There is an example batch job script here: [Example-batch-job](https://github.com/ForModLabUHel/fmi.weather.finland/blob/main/docs/batch_job_example.txt).
    2. The most efficient way of running is to use parallel processing. This can be done by setting ```--cpus-per-task=<n-cores>``` in the batch job script. The number of cores should be set according to the number of years there are to process so that ```n-cores = n-years + 1```. Maximum ```n-cores``` is ```40```. The value will also depend on the required ```--mem-per-cpu```.

    