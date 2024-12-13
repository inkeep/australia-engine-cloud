FROM --platform=linux/amd64  python:3.12-slim
ENV PYTHONUNBUFFERED Truepre
ENV PREFECT_API_URL=https://api.prefect.cloud/api/accounts/dba2ee30-9c35-4568-9f01-65994731944f/workspaces/1f5fcb8c-86dc-481c-ab32-9d48d91d3bc0
ENV PRECECT_API_KEY=pnu_njTA4ZoNFfN7cTsFMEIiCEmbwhiNML0XfZOd
ENV PREFECT_PROFILE='default'
RUN apt-get update && apt-get install -y curl && curl -fsSL https://deb.nodesource.com/setup_16.x | bash - && apt-get install -y nodejs
# Install dependencies
RUN apt-get update && apt-get install -y \
    pandoc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN npm install -g downdoc
COPY . /
WORKDIR /
RUN pip install -r requirements.txt
