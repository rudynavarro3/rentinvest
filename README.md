# rentinvest
Rental Investment Workflow for Oklahoma City Area

## Install Requirements 
#### Python Requirements
pip install requirements.txt

#### Tableau 


## Execute Script
```python3 /rentinvest/rentinvest.py```

## Setup Automation
#### Mac/Linux

1. Open a terminal window
2. Open crontab
```{.sh} crontab -e ```
3. Create a cron to execute the rentinvest.py script
```{.sh} * * * * * /usr/bin/python3 /rentinvest/rentinvest.py```
