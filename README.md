## Objective

You are assigned to the following task by our Data Science team. The goal of the task is to enable a Data Scientist to analyze historical and current energy price data from the German energy market (from now, call this the “market price data”). You are tasked with designing and implementing a solution that ensures efficient data storage, accessibility, and query capabilities, while adhering to best practices in data engineering.

### User Story

#### Story Description

- As a Data Scientist, I need to analyze the historical market price data of the German market.
- As a Data Scientist, I want to have a table containing historical data (up to 5 years) as well as data for the current month (a delay of 24 hours is acceptable).
- As a Data Scientist, I want the market price data to be persisted and queryable/accessible in AWS Athena and PySpark.
- As a Data Scientist, I want to be able to query or filter based on either a UTC-based timestamp or a local-timezone-based (Europe/Berlin) timestamp.
- As a Data Scientist, I want to be able to query `DESCRIBE <your_tablename>;` to retrieve all information, descriptions, and comments for the table, including all columns.
- As a Data Scientist, I want to join/merge the market price data with other data sources/tables (e.g. Glue tables, not part of challenge). To achieve this, the data structure should be formatted as follows:
    - Each row represents one delivery period (1 hour).
    - Each row contains the start time of the delivery period (date + time), the index price for the period, and timezone/daylight saving time information.

### Technical details

- There is an existing synchronization job (not part of this challenge) that copies market price data from an external provider to an S3 bucket with the following structure:
	- Path structure: `s3://data-landing-zone/daa_market/<year>/<month>/daa_market.csv`
	- There is one CSV file per month and year.
	- An example CSV file is provided in this repository.
	- Each row represents one delivery day (timezone: Europe/Berlin), and the columns (Hour 1, Hour 2, etc.) represent the market price for the corresponding hour of the day.
	- The columns Hour 3A and Hour 3B handle days affected by daylight saving time (DST) switches:
	    - On days with 24 hours, Hour 3A is filled, and Hour 3B is empty.
	    - On days with 23 hours, both are empty.
	    - On days with 25 hours, both are filled.
	- For the current month, the csv includes data for all past days of the month.
    - A new updated csv file is provided automatically every day at 3am UTC every day at 3am UTC. 
- The solution must run in an AWS account. (Do not assume any additional resources are pre-deployed in the account).
- There are no restrictions on using existing AWS services for the solution.
- The preferred programming language for business logic is Python and Typescript for AWS CDK.

## How to work on your submission
1. Carefully read the task above, including the technical details, and prepare a design document for a production-ready solution with an architecture diagram (including orchestration, logging, monitoring, CI/CD, etc.).
2. Implement your submission within this repository. Use the example CSV file, included in the repository, to test the behavior of your solution.
3. Add Infrastructure as Code (IaC) using AWS CDK for your solution to this repository.
4. Document your codebase and all assumptions you made.
5. Upload everything to this repository.

Bonus Tasks:
6. Add alternative architecture solutions to the design document and describe their pros and cons.
7. Implement the synchronization job (mentioned above in the technical details) fetching data from a REST-API:
	- Use the following API endpoint example as input: `https://www.example.com/api/v1/market=daa&month=<month>&year=<year>`, where `<month>` is represented as a number (1, 2, 3, etc.), and `<year>` as a 4-digit number (e.g., 2021, 2022).
	- No authentication is required.
	- The reponse is a csv file (same format as example csv)
	- Store the csv in the following path structure: `s3://data-landing-zone/daa_market/<year>/<month>/daa_market.csv`

## How we will evaluate your submission

- Data/Cloud/Software Engineering Best Practices
- Completeness: Are all features fully and correct implemented?
- Maintainability: Is the code written in a consistent, clean, testable, and maintainable manner?
- Robustness/Scalability/Cost-Effectiveness: Does the solution demonstrate reliability, scalability, and cost-efficiency?

## CodeSubmit

Please organize, design, test, and document your code as if it were
going into production - then push your changes to the master branch.

Have fun coding! 

The Entrix Team

-----------------------------------------------solution-----------------------------
Hi Team ,

Thanks for this opportunity , i enjoyed developing the project.
Please find the details attached in solution.md file.