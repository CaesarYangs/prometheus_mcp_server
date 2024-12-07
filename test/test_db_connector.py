# import sys
# import pytest
# import logging
# from src.prometheus_mcp_server.server import get_config
# from src.prometheus_mcp_server.db_connector import PrometheusHandler


# # config logging
# logging.basicConfig(
#     level=logging.DEBUG,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )
# logger = logging.getLogger("prometheus_mcp_server")
# prometheus_handler = PrometheusHandler(logger, get_config()['host'])

# def test_initialize():
#     get_config()['host']

# def test_get_all_metrics():
#     all_metrics = prometheus_handler.get_all_metrics()
#     logger.info(all_metrics)
#     return

# def test_get_range_data(metric="go_gc_heap_frees_by_size_bytes_bucket"):
#     metric_data = prometheus_handler.get_range_data(metric)
#     logger.info(metric_data)
#     return

