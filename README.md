<h2 align="center">Car Carsh Analysis - Case Study </h3>

## Overview
<p align="left"> Develop a modular application using pyspark to provide results for given tasks. This project is a scalable and modular data analysis application built with Python and Apache Spark. It processes crash datasets to derive insights about vehicle crashes, fatalities, and related statistics.    
</p>

## Requirements 

<h4></h4>
  Application should perform below analysis and store the results for each analysis in the destination folder:
	<p></p>
  <ol>
    <li>Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?</li>
    <li>Analysis 2: How many two wheelers are booked for crashes? </li> 
    <li>Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.</li>
    <li>Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run.</li>
    <li>Analysis 5: Which state has highest number of accidents in which females are not involved?.</li>
		<li>Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death.</li>
<li>Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style.</li> 
<li>Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)</li>
<li>Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance. </li>
<li>Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data).</li></ol>

## Project File Structure

The basic project structure is shown as below:
```
Crash_Analysis/
├── data/                     # Folder for datasets (input and output)
│   ├── input/                # Input datasets (e.g., Primary_Person_use.csv)
│   ├── output/               # Analysis results (saved here as CSV/txt files)
├── src/                      # Source code directory
│   ├── analysis.py           # Logic for running analyses
│   ├── config.py             # Configuration for file paths and parameters
│   ├── data_ingestion.py     # Code to load and preprocess data
│   ├── utils.py              # Utility functions (e.g., logging, validation)
├── main.py                   # Main script to orchestrate the analysis
├── requirements.txt          # Python dependencies
├── README.md                 # Project documentation

```

## Getting Started </a>

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites
<ol>
	<li>Python 3.8+ </li>
	<li>Apache Spark 3.x</li>
</ol>

### Setup and Execution

1. Clone the github repo with the URL.
```
git clone https://github.com/adityak872/Crash_Analysis.git
cd Crash_Analysis
```
2. Create a Virtual Environment: 
It is highly recommended to create and activate a Python virtual environment to isolate dependencies:
```
python3 -m venv venv
source venv/bin/activate    # On macOS/Linux
venv\Scripts\activate       # On Windows
```

3. To install all dependencies, run:
```
pip install -r requirements.txt
```
4. Check Spark is installed properly or not by running.
```
spark-shell
```
5. Run the Application:
```
spark-submit main.py
```

## Input </a>

Input datasets are stored in the data/input/ directory as csv files:
```
data/input/
├── Charges_use.csv
├── Units_use.csv
...

```
## Output </a>

Analysis results are stored in the data/output/ directory as files, with each analysis producing a separate file. Example:
```
data/output/
├── analysis_1_results.csv
├── analysis_2_results.csv
...

```

