FROM flybetter/analysis:v3
MAINTAINER flybetter@163.com
# Setup the python3 and java System

COPY . /app
WORKDIR /app
RUN python3 ./analyseUserBehavior/setup.py install
CMD ["python3","-u","./analyseUserBehavior/startup.py"]
