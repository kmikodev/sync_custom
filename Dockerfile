FROM python:3.9.16
LABEL maintainer="mcolomer@killia.com"
WORKDIR /app
COPY . .
COPY id_rsa /root/.ssh/
COPY known_hosts /root/.ssh/
RUN chmod 600 /root/.ssh/id_rsa
RUN chmod 600 /root/.ssh/known_hosts
RUN pip install -r requirements.txt
CMD ["python3","-u", "/app/apps/cron.py"]