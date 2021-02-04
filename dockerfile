FROM heroku/miniconda

WORKDIR /
COPY ./environment.yml environment.yml

RUN apt-get update && \
    apt-get install gcc -y && \
    apt-get autoremove && \
    apt-get clean && \
    conda update conda && \
    conda env create -f environment.yml && \
    conda clean --all && \
    find /opt/conda/ -follow -type f -name '*.a' -delete && \
    find /opt/conda/ -follow -type f -name '*.pyc' -delete && \
    find /opt/conda/ -follow -type f -name '*.js.map' -delete

ENV PATH /opt/conda/envs/reddit/bin:$PATH
RUN /bin/bash -c "source activate reddit"

COPY ./ /