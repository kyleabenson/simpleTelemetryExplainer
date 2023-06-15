import logging
import time
from flask import Flask
from google.cloud import bigquery
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
)
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter

# Comments to show/demo local console exporter
# trace_provider = TracerProvider()
# processor = BatchSpanProcessor(ConsoleSpanExporter())
# provider.add_span_processor(processor)

# Configure GCP Trace

tracer_provider = TracerProvider()
cloud_trace_exporter = CloudTraceSpanExporter()
tracer_provider.add_span_processor(
    # BatchSpanProcessor buffers spans and sends them in batches in a
    # background thread. The default parameters are sensible, but can be
    # tweaked to optimize your performance
    BatchSpanProcessor(cloud_trace_exporter)
)

# Sets the global default tracer provider
trace.set_tracer_provider(tracer_provider)

# Creates a tracer from the global tracer provider
tracer = trace.get_tracer("simpleExplainerTrace")


app = Flask(__name__)

@app.route('/')
def hello_world():
    with tracer.start_as_current_span("flaskRequest") as span:
        app.logger.info("Fetching a random piece of art")
        time.sleep(4)
        try:
            returnedArt = getArt()
            return '<!DOCTYPE html> <html> <p> Behold! Art: </p><img src="%s" alt="Met Art" /></html>' % (returnedArt)
        except IndexError:
            app.logger.error("Hit an error, exiting")
            abort(404)
    

def getArt():
    with tracer.start_as_current_span("getArt") as span:
        time.sleep(3)
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