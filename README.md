# Sina-MicroBlog-Potential-Customer-Mining
This project introduces how to conduct Sina Micro-Blog potential customer mining, including crawler big_V and its fans information, user blog and comment information, and user topic information.

## Code files Description, using Sina APIClient
1. big_V_fans_crawler.py is a multi-threaded crawling code file to crawl large V personal and its fans information.  
2. comment_crawler.py is a multi-threaded crawling code file to crawl user comment information.  
3. blog_crawler.py is a multi-threaded crawling code file to crawl user blog information.

## Code files Description, simulated account login instead of using Sina APIClient  
1. simulation_crawler.py is the crawling code to crawl user profiles information.  
2. topic_crawler.py is the crawling code to crawl user topic information.

## Lib files Description  
1. db_api.py is the interface code between this program and database.  
2. rsa is the module to import when the project runs.
