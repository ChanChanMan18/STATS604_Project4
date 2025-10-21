FROM jupyter/r-notebook

WORKDIR /app

COPY analyses/initial_script.R .
COPY Makefile .

CMD ["/bin/bash"]