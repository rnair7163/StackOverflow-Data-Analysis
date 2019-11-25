# StackOverflow-Data-Analysis
Analysing certain aspects of the stack overflow data

# Methodology

# 1) Hive & Pig
  # Query 1: Most Viewed Users
  Writing query for top 100 most viewed users in the user dataset, column referred in this dataset was views.

  # Query 2: Most Valuable Users
  Writing query for top 100 most valuable users in the user dataset, column referred in this was reputation which signifies the repute     of a user.

  # Query 3: Accepted Answer Percentage
  Writing query for percentage of accepted answer by joining post answers and post questions and then finding the ration between           accepted answers and total answers.  

  # Query 4: Marking Spammers
  Writing query for marking spammers by matching vote id from votes dataset id = 12 (as described by Stack Overflow community a spam)     with   user id in comments dataset and grouping into one table.

# 2) Tag Prediction
  Step 1: Importing libraris relating to PySpark
  
  Step 2: Reading the .csv file into PySpark Data Frame
  
  Step 3: Splitting the data into train and test with a ration of 75:25
  
  Step 4: Removing HTML tags from posts using BsTextExtractor
  
  Step 5: Creating list of stopwords
  
  Step 6: Defining Variables for pipeline
  
  Step 7: Building pipeline
  
  Step 8: Fitting the model
  
  Step 9: Model predicting
  
  Step 10: Model Evaluation
  

