#!/bin/bash
skrape -u $RDSUSER -H $RDSHOST -D $RDSDB -e /mnt/data/ --password=false
