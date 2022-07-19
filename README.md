# DataDive Climate

This repository contains the scripts and data necessary 
to reproduce the climate analysis for the
[Climate Data Dive page](https://www.one.org/africa/issues/covid-19-tracker/explore-climate-change/)

Maintainers:
- [Luca Picci](https://github.com/lpicci96/)

### Sources:

- Our World in Data
- World Bank, World Development Indicators (WDI)
- IMF, World Economic Outlook (WEO)
- NASA Goddard Institute for Space Studies (GISS)
- Centre for Research on the Epidemiology of Disasters (CRED)
- Notre Dame Global Adaptation Initiative (ND-GAIN)
- UN World Population Prospects
- World Mining Data

Repository Structure and Information
In order to reproduce this analysis, Python (>= 3.10) is needed. 
Other packages are listed in requirements.txt. 
The repository includes the following sub-folders:

`output`: contains clean and formatted csv files that are used to create the
visualizations. 
`raw_data`: contains raw data used for the analysis including manually downloaded data 
`glossaries`: contains metadata and other useful lookup files. 
`scripts`: scripts for creating the analysis. 
`download_data.py` contains functions to extract and clean data from sources. 
`charts.py` contains functions to produce flourish charts.
`utils.py` contains utility functions and 
`config.py` manages file paths to different folders and source urls.

### Manually downloaded data

Data from the International Disaster Database (EM-DAT) from
CRED needs to be manually downloaded from the
[data portal](https://public.emdat.be/). In the Query tool
select all options and download. Place the file in
`raw_data` as `emdat.xlsx`.

