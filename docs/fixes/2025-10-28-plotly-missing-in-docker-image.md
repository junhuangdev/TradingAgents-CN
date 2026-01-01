# Docker镜像缺少 plotly 导致首屏报错

问题日期: 2025-10-28

概述: 在使用 Docker 部署时，打开 `http://localhost:8501/` 首屏出现 `ModuleNotFoundError: No module named 'plotly'`。错误来源于 `web/components/results_display.py` 顶部导入 `plotly.graph_objects as go`，而镜像中未安装 `plotly`。

环境与版本:

- 基础镜像: `python:3.10-slim-bullseye`
- Python: `3.10.18` (`/usr/local/bin/python`)
- 服务: `TradingAgents-web`（Streamlit）
- 构建入口: `TradingAgents-CN/Dockerfile`（`pip install -r requirements.txt` + 仅校验 `streamlit`）
- Compose: `deploy/tradingagents-cn/docker-compose.yml`

复现步骤:

1. 在 `deploy/tradingagents-cn` 目录执行 `docker compose build --no-cache web`
2. 执行 `docker compose up -d`
3. 访问 `http://localhost:8501/`，出现红色错误卡片，日志中有 `ModuleNotFoundError: No module named 'plotly'`

临时解决方案:

- 在运行中的容器内安装缺失依赖并重启:
  - `docker compose exec web pip install plotly`
  - `docker compose restart web`
- 验证导入成功: `docker compose exec web python -c "import plotly, plotly.graph_objects as go; print('plotly-ok', plotly.__version__)"`

根因分析:

- `Dockerfile` 仅校验并补装 `streamlit`，若 `pip install -r requirements.txt` 因网络或镜像源不稳定导致部分包（如 `plotly`、`python-dotenv`）未安装，构建仍会继续，运行阶段报错。
- 仓库已有 `requirements-lock.txt` 和 `pyproject.toml`，但镜像构建未使用锁文件，无法保证依赖一致性。
- 构建过程中存在镜像源切换（先用清华镜像，其次官方），在清华镜像超时/失败时可能出现部分依赖未正确安装的情况。

规划的永久修复:

1. 更新 `Dockerfile`，优先使用 `requirements-lock.txt` 进行安装；若不存在则回退到 `pip install -r requirements.txt` 或 `pip install -e .`。
2. 在构建阶段显式校验关键首屏依赖并补装：
   - 必须存在: `streamlit`, `plotly`, `python-dotenv`, `pandas`, `numpy`
   - 若任一缺失，自动 `pip install` 并在失败时终止构建。
3. 改善网络稳定性与重试策略：
   - 设置 `PIP_DEFAULT_TIMEOUT`、使用 `--retries` 或通过 `uv` 保证安装稳定性。
   - 在镜像源失败时清晰输出构建日志，避免静默跳过。

验收标准:

- 构建镜像后，容器启动日志中无 `ModuleNotFoundError`。
- 健康端点 `/_stcore/health` 返回 `ok`。
- 访问 `http://localhost:8501/` 首屏无错误卡片，`results_display` 能渲染 Plotly 图表。

相关文件:

- `TradingAgents-CN/Dockerfile`
- `deploy/tradingagents-cn/docker-compose.yml`
- `web/components/results_display.py`

其他备注:

- Compose 启动时出现 `MONGODB_URL` / `REDIS_URL` 未设置的警告，不影响启动，但建议在 `deploy/tradingagents-cn/.env` 中设置以便后续启用数据库功能。