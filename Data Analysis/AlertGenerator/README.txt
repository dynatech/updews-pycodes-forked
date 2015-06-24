June 24, 2015

- This code is a quick modification for the June 26 Senslope and Dynaslope
	Project Progress report.
	
- Included in this folder is the sample output from "blcb" for July 2014
	with the label "lsb1monthblcb.csv"
	
- What if I want to select a different column sensor?
	-> Open "genLsbAlertsHistoryMonth.py"
	-> Change the value for the "targetSite" variable
	
- What if I want to change the month selection?
	-> Open "genLsbAlertsHistoryMonth.py"
	-> Look for the variable "d" just under "targetSite" and replace it
		with the format:
			date(YYYY, MM, dd)
		ex (get alerts for the month of September):
			date(2014,9,30)