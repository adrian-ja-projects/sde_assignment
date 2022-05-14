FROM python:3.9

WORKDIR /fastapi_app

COPY requirements.txt .

RUN pip install -r requirements.txt

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--reload"]