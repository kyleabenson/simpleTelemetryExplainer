from flask import Flask
from google.cloud import bigquery
import logging

app = Flask(__name__)

@app.route('/')
def hello_world():
    try:
        returnedArt = getArt()
        return '<!DOCTYPE html> <html> <p> Behold! Art: </p><img src="%s" alt="Met Art" /></html>' % (returnedArt)
    except IndexError:
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
        return
    
    try:
        results = query_job.result()  # Waits for job to complete.
        for row in results:
            queryResults = row['original_image_url']
        return queryResults
    except: 
        return

if __name__ == '__main__':
    app.run(debug=True)