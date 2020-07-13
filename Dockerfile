FROM python:3.8
RUN pip3 install pipenv
COPY Pipfile Pipfile.lock /app/
WORKDIR /app
RUN pipenv install --three --deploy --ignore-pipfile
COPY dgraph_importer /app/dgraph_importer
CMD ["pipenv", "run", "python", "-u", "-m", "dgraph_importer"]
