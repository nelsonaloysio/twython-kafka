FROM python:3.8

WORKDIR /usr/src/app

RUN pip install pipenv

COPY Pipfile ./
RUN pipenv lock && pipenv install --system

COPY . .

CMD [ "python", "./run.py"]
