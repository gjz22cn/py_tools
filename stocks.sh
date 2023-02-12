#!/bin/bash

TODAY=`date +%Y%m%d`
LOG_FILE=/var/log/stock/$TODAY.log

TYPE=$1
echo "start TYPE=$TYPE" >> $LOG_FILE
WORK_DIR=/root/code/py_tools
cd $WORK_DIR
source venv/bin/activate
cd $WORK_DIR/tools
if [[ -z "$TYPE" ]]; then
	python pre_process.py >> $LOG_FILE
	python pre_process.py >> $LOG_FILE
	python pre_process.py >> $LOG_FILE
	python ZbUtil.py >> $LOG_FILE
elif [ "$TYPE" = "download" ]; then
	python pre_process.py >> $LOG_FILE
	python pre_process.py >> $LOG_FILE
	python pre_process.py >> $LOG_FILE
elif [ "$TYPE" = "calc" ]; then
	python ZbUtil.py >> $LOG_FILE
elif [ "$TYPE" = "all" ]; then
	python pre_process.py >> $LOG_FILE
	python pre_process.py >> $LOG_FILE
	python pre_process.py >> $LOG_FILE
	python ZbUtil.py >> $LOG_FILE
fi
deactivate
echo "end TYPE=$TYPE" >> $LOG_FILE
