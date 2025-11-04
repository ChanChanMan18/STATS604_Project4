FROM jupyter/r-notebook

USER root

WORKDIR /app

COPY analyses/ ./analyses/
COPY data/ ./data/
COPY results/ ./results/
COPY outputs/ ./outputs/
COPY Makefile .

RUN chown -R ${NB_UID}:${NB_GID} /app

COPY analyses/initial_script.R .
COPY analyses/load_data.R .


CMD ["/bin/bash"]