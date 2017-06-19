#!/bin/bash

case "$1" in
start)  echo "[kafka-manager] starting kafka..."
        /opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties > /dev/null &
	sleep 5
        /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
        echo "[kafka-manager] kafka started"
        ;;
stop)   echo "[kafka-manager] stopping kafka..."
        /opt/kafka/bin/kafka-server-stop.sh
	/opt/kafka/bin/zookeeper-server-stop.sh
	/opt/kafka/bin/zookeeper-server-stop.sh
        echo "[kafka-manager] kafka stopped"
        ;;
restart) echo "[kafka-manager] restarting kafka..."
        systemctl stop kafka
        systemctl start kafka
        echo "[kafka-manager] kafka restarted"
        ;;
reload|force-reload) echo "[kafka-manager] Not yet implemented"
        ;;
*)      echo "Usage: kafka.sh {start|stop|restart}"
        exit 2
        ;;
esac
exit 0
