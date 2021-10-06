ARG DEPENDENCY_BASE_IMAGE_VERSION=2107.0.1

FROM §/com.sap.datahub.linuxx86_64/sles:${DEPENDENCY_BASE_IMAGE_VERSION}
RUN groupadd -g 1972 vflow && useradd -g 1972 -u 1972 -m vflow
USER 1972:1972
WORKDIR /home/vflow
ENV HOME=/home/vflow

RUN python3 -m pip install scipy
RUN python3 -m pip install geopy