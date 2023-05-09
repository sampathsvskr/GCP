#https://cloud.google.com/monitoring/api/metrics_gcp#gcp-dataproc

from google.cloud import monitoring_v3
import time

# specify the project ID and cluster name for your Data Proc cluster
region="your-region"
project_id = "your-project-id"
cluster_name = "your-cluster-name"

# create a client object for the Monitoring API
client = monitoring_v3.MetricServiceClient(
    client_options ={"api_endpoint" : f"{region}-monitoring.googleapis.com:443"}
)
project_name = f"projects/{project_id}"

# specify the resource type and metric name for the CPU usage metric
resource_type = "cloud_dataproc_cluster"
metric_name = "dataproc.googleapis.com/cluster/yarn/nodemanagers"

# aggregation = monitoring_v3.Aggregation(
#     'allignment_period':{"seconds":3600},
#     'per_series_aligner':monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
#     'cross_series_reducer':monitoring_v3.Aggregation.Reducer.REDUCER_MEAN,

# )


# specify a time interval for the metric data
interval = monitoring_v3.TimeInterval()
interval.end_time = monitoring_v3.Timestamp.now()
interval.start_time = interval.end_time - monitoring_v3.Duration(seconds=3600*60)

# create a query to retrieve the metric data
query = f"resource.type={resource_type} resource.labels.cluster_name={cluster_name} metric.type={metric_name}"
results = client.list_time_series(
    request = {
    "name":project_name,
    "filter":query,
    "interval": interval,
    "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
    #"aggregation":aggregation
    }
)

# print the metric data for each time series
for result in results:
    print(result)
