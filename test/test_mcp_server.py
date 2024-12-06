import sys
import pytest
import logging
from src.prometheus_mcp_server.server import get_config,list_resources,read_resource,list_tools,call_tool
from src.prometheus_mcp_server.db_connector import PrometheusHandler

# config logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("prometheus_mcp_server")
prometheus_handler = PrometheusHandler(logger, get_config()['host'])

def test_initialize():
    get_config()['host']


@pytest.mark.asyncio
async def test_list_tools():
    tools = await list_tools()
    logger.info(tools)
    assert tools[0].name == "fetch_metric"
    

@pytest.mark.asyncio
async def test_list_resources():
    resources = await list_resources()
    logger.info(resources)


@pytest.mark.asyncio
async def test_read_resource():
    url = "prometheus://go_gc_heap_frees_by_size_bytes_bucket/metric"
    resource = await read_resource(url)
    logger.info(resource)
    


@pytest.mark.asyncio
async def test_call_valid_tool(tool_name="fetch_metric"):
    argument = {
        "metric_name": "go_gc_heap_frees_by_size_bytes_bucket",
        "metric_range":""
    }
    res = await call_tool(tool_name, argument)
    logger.info(f"res:{res[0]}")


@pytest.mark.asyncio
async def test_call_invalid_tool():
    with pytest.raises(ValueError, match="Unknown tool"):
        await call_tool("invalid_tool", {})
