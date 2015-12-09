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

###Comparison with other models & methods:

Model | Whole data (Accuracy) | Whole data (Precision) | Whole data (Recall) | Downsampled bootstrap(Accuracy) | DS (Precision) | DS (Recall)
----- | --------------------- | ---------------------- | ------------------- | ------------------------------- | -------------- | -----------
Random Forest | - | - | - | 95% | 80% | 70%
Logistic Regression | - | - | - | 85% | 38% | 72%
Decision Trees | - | - | - | 95% | 78% | 70%


###Next steps:

* **Send NPS surveys to potential churners to validate model**
  1. Filter people who have bill cycle days in the next week
  2. Get their 2 month history on ticketing
  3. Find list of highly probable churners
  4. Send NPS surveys. Analyze average response with our regular nps_surveys using t-test
  
* **What do we do about people who know are going to churn?**
  1. What factors are causing them to churn?
