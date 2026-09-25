"""服务入口：装配版本化 API 网关。"""

from __future__ import annotations

import logging

import uvicorn

from gateway.api import create_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

app = create_app()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
