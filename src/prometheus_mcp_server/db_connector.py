from prometheus_api import *
# from src.prometheus_mcp_server.prometheus_api import *


class PrometheusHandler:
    def __init__(self, logger, PROMETHEUS_URL="http://localhost:9090") -> None:
        self.prom = PrometheusConnect(url=PROMETHEUS_URL)
        self.logger = logger
        self.all_metrics_full = []
        self.all_metrics_name = []

    def get_all_metrics(self):
        self.all_metrics_full = self.prom.all_metric_meta()
        return self.all_metrics_full

    def get_range_data(self, metric_name, metric_range='10', include_index=False):
        """get data from prometheus server

        Args:
            metric_name (_type_): _description_
            metric_range (str, optional): _description_. Defaults to 10, the unit is minute, default sets to 10(minutes).
            include_index (bool, optional): _description_. Defaults to False.

        Raises:
            ValueError: _description_

        Returns:
            _type_: _description_
        """
        metric_data = self.prom.get_metric_range_data(metric_name=metric_name, metric_range=metric_range, logger=self.logger)

        range_data = []
        metric_object_list = MetricsList(metric_data)

        for metric in metric_object_list:
            self.logger.info(f"{metric}")
            values = []
            for index, row in metric.metric_values.iterrows():
                timestamp = []
                value = []
                try:
                    timestamp = row['ds']
                    value = row['y']
                except Exception as e:
                    raise ValueError(f"Invalid metric value fetch")

                data_row = (timestamp, value)
                if include_index:
                    data_row = (index, timestamp, value)
                values.append(data_row)
                range_data.append(values)
        return range_data

    def test_prometheus(self, metric_name="go_gc_duration_seconds"):
        my_label_config = {'instance': 'instance_id', 'job': 'job_id', 'quantile': 'quantile_value'}
        metric_data = self.prom.get_metric_range_data(metric_name=metric_name)

        metric_object_list = MetricsList(metric_data)
        for metric in metric_object_list:
            self.logger.info(metric.label_config)
