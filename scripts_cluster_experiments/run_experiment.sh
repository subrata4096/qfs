#!/bin/bash


function doKill {
            chunkserverIP=$1
            #echo $chunkKillScriptName
            #echo $chunkRunScriptName
            ssh ubuntu@$chunkserverIP 'bash -s' < $chunkKillScriptName
}

function doSetup {
            chunkserverIP=$1
            #echo $chunkKillScriptName
            #echo $chunkRunScriptName
            #ssh ubuntu@$chunkserverIP 'bash -s' < $chunkKillScriptName
            ssh ubuntu@$chunkserverIP 'bash -s' < $chunkRunScriptName
            #com='rm -rf ~/qfsbase'
            #ssh ubuntu@$chunkserverIP 'bash -s' < $com
            #$chunkKillScriptName
            #$chunkRunScriptName
            #exit
}


chunkRunScriptName="/home/ubuntu/launch_chunk_server.sh"
metaRunScriptName="/home/ubuntu/launch_meta_server.sh"

chunkKillScriptName="/home/ubuntu/kill_chunk_server.sh"
metaKillScriptName="/home/ubuntu/kill_meta_server.sh"

mode=$1
qfscodedirname=""
if [ "$mode" == "orig" ]
then 
   	echo "Running Original QFS"
  	chunkRunScriptName="/home/ubuntu/launch_chunk_server.sh"
	metaRunScriptName="/home/ubuntu/launch_meta_server.sh"

	chunkKillScriptName="/home/ubuntu/kill_chunk_server.sh"
	metaKillScriptName="/home/ubuntu/kill_meta_server.sh"
	
	qfscodedirname="qfs-original"

elif [ "$mode" == "repair" ]
then
	echo "Running Repair QFS"
	chunkRunScriptName="/home/ubuntu/launch_chunk_server_repair.sh"
	metaRunScriptName="/home/ubuntu/launch_meta_server_repair.sh"

	#chunkKillScriptName="/home/ubuntu/kill_chunk_server_repair.sh"
	#metaKillScriptName="/home/ubuntu/kill_meta_server_repair.sh"
	chunkKillScriptName="/home/ubuntu/kill_chunk_server.sh"
	metaKillScriptName="/home/ubuntu/kill_meta_server.sh"
	
	qfscodedirname="qfs-repair"
else
   exit 1
fi

qfsdir="/home/ubuntu/codes/"$qfscodedirname

#$metaKillScriptName
#$qfsdir/examples/sampleservers/sample_setup.py -a uninstall
#rm -rf ~/qfsbase

#$qfsdir/examples/sampleservers/sample_setup.py -a install
 
#$metaRunScriptName

chunkservers="192.168.1.225 192.168.1.226 192.168.1.227 192.168.1.228 192.168.1.229 192.168.1.230 192.168.1.231 192.168.1.233 192.168.1.235 192.168.1.236"

for f in $chunkservers
do
	echo "Processing $f"
        scp $chunkKillScriptName ubuntu@$f:/home/ubuntu/
        doKill $f
done

$qfsdir/examples/sampleservers/sample_setup.py -a uninstall

$qfsdir/examples/sampleservers/sample_setup.py -a install


for f in $chunkservers
do
        echo "Processing $f"
        scp $chunkRunScriptName ubuntu@$f:/home/ubuntu/
        doSetup $f
done
