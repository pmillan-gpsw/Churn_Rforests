# Churn_Rforests
Churn model using ensemble of down sampled decision trees

Features:
	a. Most recent ticket type
	b. Days since last ticket
	c. Customer life since activation
	d. Device switch
	e. Port flag
	f. Multiline flag
	g. Plan
	h. Device life

<i>ADD MORE AS NEEDED</i>	
########################################################################	

Assumptions:
	a. Devices are grouped into 3 categories depending on our portfolio 
		(Something to explore expanding into if need be)
	b. Activations after December 2013 only included just to avoid Defy 
		influence
	

########################################################################
	
	
Model Design:
Parameters for model: 
	a. Number of trees or Number of bootstraps
	b. Majority vote % threshold
	c. Downsample percentage minority
########################################################################	

