#!/bin/sh

# login
vctl login https://vsystem.ingress.xxxxxx.dhaas-live.shoot.live.k8s-hana.ondemand.com\
 default user -p pwd

# export
TARGET="solutions/$2.tar.gz"
echo "vctl cmd: vctl vcrep user export $2 $1"
vctl vrep user export $TARGET $1

# push gi
git add -f $TARGET
git commit -m "diupdate"
git push origin main