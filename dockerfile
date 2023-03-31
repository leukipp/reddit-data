FROM python:3.11-slim-bullseye

WORKDIR /tmp
COPY . .

RUN apt-get update && apt-get install -y build-essential libsnappy-dev
RUN pip install -r requirements.txt

CMD ["python", "data.py"]