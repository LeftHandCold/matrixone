FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir aiohttp
COPY obs_proxy.py .
EXPOSE 9000
ENTRYPOINT ["python", "obs_proxy.py"]
