FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY adapter.py server.py tool_dsml.py tool_sieve.py config_tool.py ./

ENV HOST=0.0.0.0 \
    PORT=8000

EXPOSE 8000

CMD ["python", "server.py"]
