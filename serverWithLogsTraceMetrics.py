import logging
import time
from flask import Flask
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from prometheus_client import make_wsgi_app
from google.cloud import bigquery


###################################
# OTEL GCP Trace Libraries
###################################
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
)
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter

# ###################################
# # Otel GCP Monitoring Libraries
# ###################################
# from opentelemetry import metrics
# from opentelemetry.sdk.metrics import MeterProvider
# from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
# from opentelemetry.exporter.cloud_monitoring import (
#     CloudMonitoringMetricsExporter,
# )
# from opentelemetry.sdk.resources import Resource


# ###################################
# # Add Prometheus instrumentation
# ###################################
from prometheus_client import Counter



# ###################################
# # Manually Configure GCP Otel Monitoring
# ###################################
# metrics.set_meter_provider(
#     MeterProvider(
#         metric_readers=[
#             PeriodicExportingMetricReader(
#                 CloudMonitoringMetricsExporter(), export_interval_millis=5000
#             )
#         ],
#         resource=Resource.create(
#             {
#                 "service.name": "basic_metrics",
#                 "service.namespace": "examples",
#                 "service.instance.id": "instance123",
#             }
#         ),
#     )
# )
# meter = metrics.get_meter(__name__)

# # Creates metric workload.googleapis.com/request_counter with monitored resource generic_task
# requests_counter = meter.create_counter(
#     name="request_counter",
#     description="number of requests",
#     unit="1",
# )

# staging_labels = {"environment": "prod"}



###################################
# Manually Configure GCP Trace
###################################

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
c = Counter('my_requests_total', 'HTTP Failures', ['method', 'endpoint'])


@app.route('/')
def hello_world():
    with tracer.start_as_current_span("flaskRequest") as frontend:
        # requests_counter.add(1, staging_labels)
        ctx = trace.get_current_span().get_span_context()
        c.labels('get', '/').inc(exemplar={'trace_id': str(ctx.trace_id),'span_id': str(ctx.span_id)})
        app.logger.info("Fetching a random piece of art")
        time.sleep(4)
        try:
            returnedArt = getArt()
            return '<!DOCTYPE html> <html> <p> Behold! Art: </p><img src="%s" alt="Met Art" /></html>' % (returnedArt)
        except IndexError:
            app.logger.error("Hit an error, exiting")
            abort(404)
    

def getArt():
    with tracer.start_as_current_span("getArt") as backend:
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


app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {
    '/metrics': make_wsgi_app()
})

if __name__ == '__main__':
    app.run(debug=True)