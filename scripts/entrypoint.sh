#!/bin/bash

# Configuration
HADOOP_HOME="/opt/hadoop"
export PATH=$HADOOP_HOME/bin:$PATH

# Ensure data directories are writable by hadoop user
# Docker named volumes are created as root, causing permission denied for hadoop user.
if [ -d "/tmp/hadoop-hadoop" ]; then
    echo "Fixing permissions for /tmp/hadoop-hadoop..."
    chown -R hadoop:users /tmp/hadoop-hadoop
fi
if [ -d "/home/hadoop/notebooks" ]; then
    chown -R hadoop:users /home/hadoop/notebooks
fi

# Function to start Jupyter
start_jupyter() {
    echo "Starting JupyterLab setup..."
    
    # Install Python 3 if missing
    if ! command -v python3 &> /dev/null; then
        echo "Fixing CentOS 7 runtimes (EOL)..."
        sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
        sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*
        
        echo "Installing Python 3..."
        yum install -y python3
    fi
    
    # Upgrade pip and install JupyterLab
    if ! pip3 show jupyterlab &> /dev/null; then
        echo "Installing JupyterLab..."
        pip3 install --upgrade pip
        pip3 install jupyterlab
    fi
    
    echo "Launching JupyterLab..."
    # Ensure notebook directory exists
    mkdir -p /home/hadoop/notebooks
    chown -R hadoop:users /home/hadoop/notebooks
    
    # Run as root is fine for Jupyter if we want, or switch. 
    # Config asked for allow-root, so we keep it root for simplicity/install access.
    jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --notebook-dir=/home/hadoop/notebooks
}

# Function to start NameNode
start_namenode() {
    echo "Starting NameNode..."
    # Directory path depends on hdfs-site.xml, but default is /tmp/hadoop-hadoop/dfs/name
    
    if [ ! -f "/tmp/hadoop-hadoop/dfs/name/current/VERSION" ]; then
        echo "NameNode not formatted. Formatting now..."
        # Format as hadoop user
        su -c "$HADOOP_HOME/bin/hdfs namenode -format -force" hadoop
    fi
    # Run as hadoop user
    su -c "$HADOOP_HOME/bin/hdfs namenode" hadoop
}

service=$1
echo "Starting service: $service"

case "$service" in
    namenode)
        start_namenode
        ;;
    datanode)
        su -c "$HADOOP_HOME/bin/hdfs datanode" hadoop
        ;;
    resourcemanager)
        su -c "$HADOOP_HOME/bin/yarn resourcemanager" hadoop
        ;;
    nodemanager)
        su -c "$HADOOP_HOME/bin/yarn nodemanager" hadoop
        ;;
    historyserver)
        su -c "$HADOOP_HOME/bin/mapred --daemon start historyserver" hadoop
        tail -f /dev/null # Keep container alive
        ;;
    jupyter)
        start_jupyter
        ;;
    *)
        echo "Usage: $0 {namenode|datanode|resourcemanager|nodemanager|jupyter}"
        exit 1
        ;;
esac
