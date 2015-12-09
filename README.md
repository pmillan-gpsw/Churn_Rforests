# Churn_Rforests
###Goal:

Predict probability that a customer will churn looking at 
his recent *(going back 60 days from deactivation date or current date
if he is an active customer)* ticketing activity, his service line history

###Data:
* **Serviceline data** : *customer_view*
  * Customer life -> Days since activation
  * Current device type -> Moto E, G or X 
  * Current plan 
  * Device switch flag -> Current device different from the one 
		used for first activation
  * Device life -> Days since current device activation
  * Last bill date -> (Not a model variable)
  * Churn flag
  * Multiline flag	
* **Ticket data** : *zd_view*
  * Number of tickets : past 60 days 
  * Group name
  * Request type
  * Ticket class
  * Ticket type

*as of Dec 8. Add more as needed*	
	

###Assumptions:
1. Timeline of data : **Dec, 2013 to Jan, 2015**
2. Validation set outside the time range is Jan 2015 - August 2015
3. **Last bill date** is the date used to normalize the customers
		(Issue: 180k records after Moto X launch)
	
	
###Model Design:
######Data split: 
![Data split](/Screenshots/datasplit.png)


###Results:
![Results](/Screenshots/Result.png)


