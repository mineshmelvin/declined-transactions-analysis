# Declined Transactions Analysis

This Spark Scala application is a replication of a production application
usually used in a financial services sector.

The idea is:
1. Read declined credit/debit card transactions from a Kafka topic
2. Read customer data from MySQL table (not ready yet)
3. Read rules table from MySQL table (not ready yet)
4. Enrich the transaction data with all required columns
5. Perform some analysis like total declines per decline_code
6. Write results to MySQL table, Kafka topic, or console based on the account_type

This is useful if you want to take some near real time actions on the declines. \n
Example: You can send a notification to the customer's card mobile app informing him of the reason for the decline.