import uvicorn
from tests.integration.fixtures.control_panel import create_app

if __name__ == "__main__":
    app = create_app(cfg_path="tests/integration/demo/configs/demo_redis_channel_docker.toml")
    uvicorn.run(app, host="0.0.0.0", port=8080)