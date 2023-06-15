from flask import Flask
from google.cloud import bigquery
import logging

app = Flask(__name__)

@app.route('/')
def hello_world():
    app.logger.info("Fetching a random piece of art")
    try:
        returnedArt = getArt()
        return '<!DOCTYPE html> <html> <p> Behold! Art: </p><img src="%s" alt="Met Art" /></html>' % (returnedArt)
    except IndexError:
        app.logger.error("Hit an error, exiting")
        abort(404)
    

def getArt():
    try:
        client = bigquery.Client()
        queryResults = []
        query_job = client.query(
        """
        SELECT original_image_url FROM `bigquery-public-data.the_met.images` ORDER BY RAND ()  LIMIT 1"""
        )
    except:
        logging.error("Failed to establish a client, likely credentials here")
    
    try:
        results = query_job.result()  # Waits for job to complete.
        for row in results:
            queryResults = row['original_image_url']
        return queryResults
    except: 
        logging.error("No results, likely previously failed connection")


if __name__ == '__main__':
    app.run(debug=True)