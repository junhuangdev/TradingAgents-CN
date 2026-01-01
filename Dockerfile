# 使用官方Python镜像替代GitHub Container Registry
FROM python:3.10-slim-bullseye

# 安装 uv 包管理器（使用官方 PyPI 提升稳定性）
RUN pip install -i https://pypi.org/simple uv

WORKDIR /app

RUN mkdir -p /app/data /app/logs

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

## 使用官方默认 apt 源以提高稳定性（避免镜像站不稳定导致构建失败）

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    wkhtmltopdf \
    xvfb \
    fonts-wqy-zenhei \
    fonts-wqy-microhei \
    fonts-liberation \
    pandoc \
    procps \
    && rm -rf /var/lib/apt/lists/*

# 启动Xvfb虚拟显示器（启动前清理可能残留的锁文件）
RUN echo '#!/bin/bash\nrm -f /tmp/.X99-lock || true\nXvfb :99 -screen 0 1024x768x24 -ac +extension GLX &\nexport DISPLAY=:99\nexec "$@"' > /usr/local/bin/start-xvfb.sh \
    && chmod +x /usr/local/bin/start-xvfb.sh

COPY requirements.txt .

# 使用 pip 安装依赖（优先清华镜像，其次官方源）
RUN set -e; \
    for src in \
        https://pypi.tuna.tsinghua.edu.cn/simple \
        https://pypi.org/simple; do \
      echo "Try installing from $src"; \
      pip install -r requirements.txt -i $src && break; \
      echo "Failed at $src, try next"; \
    done

# 验证并确保安装 streamlit（防止镜像中缺失导致运行失败）
RUN python -c "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec('streamlit') else 1)" \
    || (pip install streamlit -i https://pypi.org/simple \
        || pip install streamlit -i https://pypi.tuna.tsinghua.edu.cn/simple)

# 复制日志配置文件
COPY config/ ./config/

COPY . .

EXPOSE 8501

CMD ["python", "-m", "streamlit", "run", "web/app.py", "--server.address=0.0.0.0", "--server.port=8501"]
