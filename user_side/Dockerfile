FROM python:3.7.2-alpine

WORKDIR /opt/parking

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "./main.py"]