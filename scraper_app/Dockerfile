FROM python:3.8

WORKDIR /app

RUN python -m pip install --upgrade pip
RUN pip install --upgrade pip setuptools wheel

COPY requirements.txt /app/
RUN pip install -r requirements.txt

COPY start-app.sh /start-app.sh
RUN chmod +x /start-app.sh

COPY ./app /app
ENV PYTHONPATH=/app