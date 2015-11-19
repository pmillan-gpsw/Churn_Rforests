# Churn_Rforests
Churn model using ensemble of down sampled decision trees

###Features:
	1. Most recent ticket type
	2. Days since last ticket
	3. Customer life since activation
	4. Device switch
	5. Port flag
	6. Multiline flag
	7. Plan
	8. Device life

*as of Nov 19. Add more as needed*	
	

###Assumptions:
	1. Devices are grouped into 3 categories depending on our portfolio 
		(Something to explore expanding into if need be)
	2. Activations after December 2013 only included just to avoid Defy 
		influence
	
	
###Model Design:
**Parameters for model**: 
	1. Number of trees or Number of bootstraps
	2. Majority vote % threshold
	3. Downsample percentage minority

