FROM python:3.12

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY main.py .

ENV TWITCH_CLIENT_ID=""
ENV TWITCH_CLIENT_SECRET=""

ENTRYPOINT ["python3", "./main.py"]