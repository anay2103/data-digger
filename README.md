# data-digger

This is a programming beginner's project aimed at financial news sentiment analysis:wink:

The project is inspired by: https://towardsdatascience.com/reading-the-markets-machine-learning-versus-the-financial-news-bcd3704f37b8.

Currently data collection stage is implemented, as well as some data pre-processing features.


## Prerequisites
The project uses a single source of financial news: https://seekingalpha.com/.

Free Alpaca Stock Trading REST API is used as a source for historical stock quotes. 

MongoDB Atlas cloud is chosen as a data storage.  

Only Windows OS is currently supported.


## Installing
    pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple data-digger
 
Get Alpaca API keys here: https://alpaca.markets/docs/.

Register a MongoDB Atlas account, create a cluster and a database: https://docs.atlas.mongodb.com/getting-started/ 

Save Alpaca API keys, Mongo URI, a database name and a collection name as environment variables in a .env file.
Distribution package contains a sample .env file filled with fake values, replace them with real ones to make the project runnable.

## Using
To scrape news articles and dump them to the database: 

    main.py --articles
    
To clean up the database and delete articles useless for sentiment analysis (updates quoting the prices at which specific securities are being sold):

    main.py --sweeper
    
To fetch historical quotes of stocks an article largely relates to, before and after the publication date:

    main.py --quotes

To label the articles with either 1 or 0 tag, depending on whether a publication was followed by abnormal returns of a stock the article refers to:

    main.py --labels

## Contributing 

If you have any ideas how to make this better, feel free to submit a pull request or contact me at yana-karausheva@yandex.ru


