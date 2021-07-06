# syntax=docker/dockerfile:1
FROM python:3-onbuild

RUN pip install --upgrade pip

COPY apicall1.py .
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "./apicall1.py"]