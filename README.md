# Data Engineering Project - Ready D24 &nbsp; &nbsp; &nbsp;<img align="center" width="50" alt="image" src="https://github.com/Ready-Talent/data-engineering-project-template/assets/70844012/4c10ad15-6b48-4ce3-9829-9e823191b410">

## Project Description 
You are looking to buy a new car, and as a data expert you decide you want to make the most informed decision by scraping car listings from 2 different websites. The data you scraped is stored in a GCS bucket in a CSV format, and you want to create a dashboard that answers a couple of questions you have.
<br>
<br>

### 1- Transfers:
The data that you will work with is stored in a GCS bucket. You are expected to move this data from GCS to BigQuery using Apache Airflow as your orchestrator and load them into landing table(s) in BigQuery.
<br>
<br>

### 2- Modeling:
You are expected to apply the necessary modeling techniques to transform the raw data in the landing tables to multiple dimension and fact tables and store them in BigQuery. The modeling part must be done using DBT only.
<br>
<br>

### 3- Reporting:
You are expected to build views using DBT from the dimension and fact tables and use them to build a dashboard on Looker Studio that answers the following questions: 

1. Top 5 car brands with the most listings
2. Best 10 fuel economy (min_mpg) gasoline cars that are manufactured not before 2019
3. Percentage of gasoline, hybrid and electric cars
4. Cheapest 10 automatic gasoline cars with mileage less than 50,000 and built not before 2019
<br>

### Additional information:
- You can find the raw data in the GCS bucket called ready-project-dataset in our new GCP project ready-data-de24
- Your code is expected to follow the clean coding guidelines discussed in previous sessions (folder naming and hierarchy, variable naming, no unused import etc.).
- You are expected to follow Github best practices ( New branch with a clear and descriptive name, short and clear commit message and Good PR description)
- Keep the commits and history in the project. don't push the whole code in one commit
- Don't push your commits directly to main branch, Create a branch for the relevant changes/features then do a pull request from the feature branch into the main branch
- You are expected to create an ERD for your models and a document explaining your approach to the project
- Testing is done in your local Airflow, for the final version we’ll turn on the composer so you can experience the production release and code reviews.

For any questions, don’t hesitate to use our slack channel to ask for help

### Deliverables:
- Architecture diagram (ERD)
- Project documentation
- Final presentation
