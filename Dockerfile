FROM ubuntu:18.04
RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
       python3 \
       python3-setuptools \
       python3-pip \
       python3-pandas \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade pip

COPY . /veoibd-data-submission
WORKDIR /veoibd-data-submission
RUN pip install -r requirements.txt
RUN python3 setup.py install
