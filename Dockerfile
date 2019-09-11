FROM flybetter/analysis:v27
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
ENV active="production"

# sudo docker build -t flybetter/analysis:v28 .

# sudo docker run -d --env active=develop --add-host cdh1:192.168.10.164 --add-host  cdh2:192.168.10.163 --add-host cdh3:192.168.10.166 flybetter/analysis:v23

#sudo docker run -d  --add-host cdho1.prod.house365:172.17.1s.141 --add-host cdho2.prod.house365:172.17.1.142 flybetter/analysis:v28

CMD ["python3","-u","./analyseUserBehavior/startup.py"]
