FROM artifactory.teslamotors.com:2153/atm-baseimages/python:3.7-xray AS builder
WORKDIR /usr/src/app
RUN pip install --upgrade pip
COPY app/requirements.txt ./
RUN pip wheel --wheel-dir=/usr/src/app/wheels -r requirements.txt

FROM artifactory.teslamotors.com:2153/atm-baseimages/python:3.7-xray
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
COPY --from=builder /usr/src/app/wheels /wheels
COPY app/requirements.txt ./
RUN pip install --no-index --find-links=/wheels -r requirements.txt
COPY ./app /app

WORKDIR /app
ENTRYPOINT ["python"]
CMD ["./main.py"]