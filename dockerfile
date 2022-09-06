FROM artifactory.teslamotors.com:2153/atm-baseimages/python:3.7-xray
COPY ./app /app
WORKDIR /app
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
ENTRYPOINT ["python"]
CMD ["./main.py"]