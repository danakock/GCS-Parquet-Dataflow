FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

WORKDIR /app

ENV PYTHONPATH=/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN pip install .

ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/app/main.py