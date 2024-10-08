FROM public.ecr.aws/lambda/python:3.10

COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r requirements.txt

# Cloning the Stockfish source code and compile the executable.
RUN yum install -y \
    gcc \
    gcc-c++ \
    make \
    git
RUN git clone https://github.com/official-stockfish/Stockfish.git /stockfish
WORKDIR /stockfish
RUN git checkout tags/sf_17 -b stockfish-17
RUN cd src && make build ARCH=x86-64-modern
RUN mv src/stockfish /usr/local/bin/stockfish && chmod +x /usr/local/bin/stockfish

COPY lambda_function.py ${LAMBDA_TASK_ROOT}
WORKDIR ${LAMBDA_TASK_ROOT}
CMD [ "lambda_function.handler" ]

