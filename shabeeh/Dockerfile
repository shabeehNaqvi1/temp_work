FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt requirements.txt
COPY final_code.py final_code.py
COPY .env .env
COPY weighty-gasket-437422-h6-0d4f22d67dc3.json weighty-gasket-437422-h6-0d4f22d67dc3.json

RUN pip install --no-cache-dir -r requirements.txt

CMD ["sh", "-c", "python final_code.py && tail -f /dev/null"]
