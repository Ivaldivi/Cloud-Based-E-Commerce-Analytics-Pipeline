#Building a Cloud-Based E-Commerce Analytics Pipeline
Author: Izzy Valdivia
### Overview
As part of the Autumn 2025 Scalable Data Systems and Algorithms course, I built a production-grade analytics pipeline that processes large volumes of artificial e-commerce event data and transforms it for business analytics. I was given a starter CloudFormation template that generates a stack to set up the data generation pipeline. This data generation pipeline simulates a situation I might find when out in the proverbial professional ‘wild’ mimicking a high volume raw data that needs a scalable solution.
To tackle the issue, I extended the starter template to include resources to ingest, process, restructure and query the data. This involved an AWS glue job, glue crawler, two event triggers, an s3 bucket, and an athena work group. In the attached writeup, I discuss the pipeline in more detail as well as walk through my decision making process with respect to each of the resources I implemented.
