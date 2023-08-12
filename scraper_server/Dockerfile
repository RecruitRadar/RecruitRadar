FROM python:3.10-slim 

RUN useradd -r -s /bin/bash jd_scraper

# set app config option
ENV FastAPI_ENV=production

# set argument vars in docker-run command
ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_DEFAULT_REGION

ENV AWS_ACCESS_KEY_ID $AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY $AWS_SECRET_ACCESS_KEY
ENV AWS_DEFAULT_REGION $AWS_DEFAULT_REGION

# Set a directory for the app
WORKDIR /app

# Install Python dependencies
COPY requirements.txt ./requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r ./requirements.txt

RUN pip install uvicorn

# Make static directory
RUN if [ ! -d "/static" ]; then mkdir /static; fi

# Copy the rest of the application's code
COPY . .

EXPOSE 8000
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
CMD ["python", "server.py"]