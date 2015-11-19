# Churn_Rforests
Churn model using ensemble of down sampled decision trees

<b>Features:</b></br></br>
	a. Most recent ticket type</br>
	b. Days since last ticket</br>
	c. Customer life since activation</br>
	d. Device switch</br>
	e. Port flag</br>
	f. Multiline flag</br>
	g. Plan</br>
	h. Device life</br>

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

