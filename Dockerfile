FROM python:3.11-alpine
WORKDIR /code
COPY . .

RUN python -m pip install --upgrade pip
RUN pip install --extra-index-url https://alpine-wheels.github.io/index -r "requirements.txt"

RUN adduser -D -h /code appuser
USER appuser

CMD python main.py