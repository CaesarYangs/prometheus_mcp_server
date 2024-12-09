import os
import asyncio
import logging
from pydantic import AnyUrl
from mcp.server import Server
from mcp.types import Resource, Tool, TextContent
from mcp.server.stdio import stdio_server
# from src.prometheus_mcp_server.db_connector import PrometheusHandler
from db_connector import PrometheusHandler

# config logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("prometheus_mcp_server")


def get_config():
    """Get basic config of prometheus and mcp
    """
    config = {
        "host": os.getenv("PROMETHEUS_HOST", "http://localhost:9090")
    }
    return config


app = Server("prometheus_mcp_server")
prometheus_handler = PrometheusHandler(logger, get_config()['host'])


@app.list_resources()
async def list_resources() -> list[Resource]:
    try:
        all_metrics = prometheus_handler.get_all_metrics()
        resources = []

        for metric in all_metrics:
            resources.append(
                Resource(
                    uri=f"prometheus://{metric}/metric",
                    name=f"{metric}",
                    mimeType="text/plain",
                    description=f"{all_metrics[metric][0]['help']}"
                )
            )
        return resources
    except Exception as e:
        logger.error(f"Failed to list all resources from prometheus {str(e)}")
        return []

@app.read_resource()
async def read_resource(uri: AnyUrl) -> str | bytes:
    try:
        uri_str = str(uri)
        logger.info(f"Reading resource: {uri_str}")
        
        if not uri_str.startswith("prometheus://"):
            raise ValueError(f"Invalid URI scheme: {uri_str}")
        
        parts = uri_str[13:].split('/')
        metric_name = str(parts[0])
        
        logger.info(f"name:{metric_name}")
        
        value = prometheus_handler.get_range_data(metric_name)
        return value
    except Exception as e:
        logger.error(f"Failed to list all resources from prometheus {str(e)}")
        raise RuntimeError(f"Prometheus error")
        return []


@app.list_tools()
async def list_tools() -> list[Tool]:
    logger.info("Listing tools...")
    return [
        Tool(
            name="fetch_metric",
            description="Fetches metric and returns its content",
            inputSchema={
                "type": "object",
                "required": ["metirc_name"],
                "properties": {
                    "metric_name": {
                        "name": "string",
                        "description": "metric to fetch",
                    },
                    "metric_range": {
                        "name": "int",
                        "description": "specific range of metric to fetch(number of minutes)",
                    }
                },
            },
        )
    ]


@app.call_tool()
async def call_tool(
    name: str, arguments: dict
) -> list[TextContent]:
    logger.info(f"Calling tool:{name} with arguments:{arguments}")

    # if name != "fetch_metric":
    #     raise ValueError(f"Unknown tool:{name}")

    try:
        metric_name = arguments['metric_name']
        metric_range = arguments['metric_range']

        value = prometheus_handler.get_range_data(metric_name=metric_name, metric_range=metric_range)

        return [TextContent(type="text", text=f"metric:{metric_name} range value return:{value}")]

    except Exception as e:
        logger.error(f"Error when fetching metric:{name} with arguments:{arguments}")
        return [TextContent(type="text", text=f"Error when fetching metric:{name} with arguments:{arguments}. error:{str(e)}")]


async def main():
    """Main entry point to run the MCP server."""
    logger.info("starting prometheus mcp server...")

    # for test
    # prometheus_handler.get_range_data("go_gc_duration_seconds")
    config = get_config()
    logger.info(f"Prometheus config:{config}")

    async with stdio_server() as (read_stream, write_stream):
        try:
            await app.run(
                read_stream,
                write_stream,
                app.create_initialization_options()
            )
        except Exception as e:
            logger.error(f"Server error:{str(e)}")
            raise

if __name__ == "__main__":
    asyncio.run(main())
