#!/bin/bash

./kill_chunk_server.sh

mkdir -p /home/ubuntu/qfsbase/chunkdir11
'/home/ubuntu/codes/qfs-original/build/debug/bin/chunkserver' '/home/ubuntu/ChunkServer.prp' '/home/ubuntu/qfsbase/ChunkServer.log' > '/home/ubuntu/qfsbase/ChunkServer.out' 2>&1 &
echo "Launched chunkserver with pid:"
cat qfsbase/chunkserver.pid
echo "With properties:"
cat /home/ubuntu/ChunkServer.prp
