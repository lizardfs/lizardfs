FROM python:3.10-slim
RUN useradd -ms /bin/sh runner
USER runner
WORKDIR /code
ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV PATH=/home/runner/.local/bin:$PATH
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
EXPOSE 5000
COPY --chown=runner:runner . ./
CMD [ "waitress-serve", "--port=5000", "--threads=1", "--call", "app:create_app" ]
