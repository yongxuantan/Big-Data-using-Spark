## User Recommender System Based on Yelp Data project
#### Goals:
- Applying Apache Spark's GraphX API
- Applying Apache Spark's SVD library
- Build an SBT project to be executed on AWS EMR cluster
- Store data and output using AWS S3 service


#### Input files: 
- Data can be downloaded from the below link by providing an email address: 
  - (http://www.yelp.com/dataset_challenge)
  - It should contain at minimal the following files: 
    - business.json 
    - review.json 
    - user.json 
 
#### Instruction: 
  - To execute the SVD algorithm and generate output, submit Spark application with parameters: <br/>
      --class “SVD” <br/>
      --File: projectx_2.11-0.1-1.jar <br/>
      --Parameters: <br/>
        - *path-to-review.json* <br/>
        - *path-to-output-directory* <br/>
        - *sample-factor* : value between 0 and 1, used to set what fraction of data to run on; for all data, set to 1 
 

  - To execute the GraphX algorithms and generate output, submit Spark application with parameters: <br/>
      --class “RunExample” <br/>
      --File: projectx_2.11-0.1-1.jar <br/>
      --Parameters: <br/>
        - *path-to-user.json* <br/>
        - *path-to-output-directory* <br/>
        - *sample-factor* : value between 0 and 1, used to set what fraction of data to run on; for all data, set to 1 
 
