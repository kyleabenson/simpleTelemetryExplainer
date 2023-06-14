from flask import Flask
from google.cloud import bigquery



app = Flask(__name__)

@app.route('/')
def hello_world():
    getAq()
    app.logger.info("Received request")
    return 'Hello, World!'

def getAq():
    client = bigquery.Client()
    query_job = client.query(
        """
        SELECT * FROM `bigquery-public-data.openaq.global_air_quality` WHERE country = "US" LIMIT 1"""
    )

    results = query_job.result()  # Waits for job to complete.

    for row in results:
        app.logger.info(row)
    return


if __name__ == '__main__':
    app.run(debug=True)
