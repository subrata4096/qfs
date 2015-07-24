#!/bin/bash

echo "KILLING the metaserver:" 
cat /home/ubuntu/qfsbase/meta/metaserver.pid
cat /home/ubuntu/qfsbase/meta/metaserver.pid | xargs kill
