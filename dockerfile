FROM artifactory.teslamotors.com:2149/python:3.7.6-buster
COPY ./app /app
WORKDIR /app
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
ENTRYPOINT ["python"]
CMD ["./main.py"]