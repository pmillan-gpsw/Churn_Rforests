# Churn_Rforests
###Goal:

Predicting the probability that a customer will churn looking at 
his recent (going back 60 days from deactivation date or current date
if he is an active customer) ticketing activity, his service line history

###Data:
	1. Serviceline data : customer_view
		* Customer life -> Days since activation
		* Current device type -> Moto E, G or X 
		* Current plan 
		* Device switch flag -> Current device different from the one 
			used for first activation
		* Device life -> Days since current device activation
		* Last bill date -> (Not a model variable)
		* Churn flag
		* Multiline flag
		
		
	2. Ticket data : zd_view
		* Number of tickets : past 60 days 
		* Group name
		* Request type
		* Ticket class
		* Ticket type

*as of Dec 8. Add more as needed*	
	

###Assumptions:
	1. Devices are grouped into 3 categories depending on our portfolio 
		(Something to explore expanding into if need be)
		1. Moto E = 1
		2. Moto G = 2
		3. Moto X = 3
	2. Activations after December 2013 only included just to avoid Defy 
		influence till Jan 2015. (Train set and Test set)
	3. Validation set outside the time range is Jan 2015 - August 2015
	4. Last invoice date is the date used to normalize the customers
		(Issue: 307k records after Moto X launch)
	5. Tickets: Ticket type, classification, triage
		1. Triage : Devc, acct, text, dvhw, cllc, ncng, misc, bill, ordr, crr,data, spam
		2. Request type: Remove tickets with request type: rw_cancel
		3. Group name: Remove tickets with group name: Cancellations
		4. ticket class: Remove tickets with ticket class: rw_cancel_manual rw_cancel_automated
	
	
###Model Design:
######Parameters for model: 
	1. Number of trees or Number of bootstraps
	2. Majority vote % threshold
	3. Downsample percentage minority

