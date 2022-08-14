FROM python:3.10-slim-buster

WORKDIR /app

COPY py_server/requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY py_server .
ENV PORT $PORT

CMD [ "python3", "main.py"]