FROM python:3.7.6-buster

COPY . /app

WORKDIR /app

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

ENTRYPOINT ["python"]

CMD ["./main.py"]