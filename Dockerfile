FROM flybetter/analysis:v11
MAINTAINER flybetter@163.com
# Setup the python3 and java System

COPY . /app
WORKDIR /app
RUN python3 ./analyseUserBehavior/setup.py install
#RUN mv /etc/apt/sources.list /etc/apt/sources.list.bak \
#    && echo "deb http://mirrors.163.com/debian/ jessie main non-free contrib" >/etc/apt/sources.list \
#    && echo "deb http://mirrors.163.com/debian/ jessie-proposed-updates main non-free contrib" >>/etc/apt/sources.list \
#    && echo "deb-src http://mirrors.163.com/debian/ jessie main non-free contrib" >>/etc/apt/sources.list \
#    && echo "deb-src http://mirrors.163.com/debian/ jessie-proposed-updates main non-free contrib" >>/etc/apt/sources.list
#RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys  8B48AD6246925553 \
#    && apt-key adv --keyserver keyserver.ubuntu.com --recv-keys  7638D0442B90D010
#RUN apt-get update && apt-get install -y vim
CMD ["python3","-u","./analyseUserBehavior/startup.py"]
