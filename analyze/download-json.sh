
#!/bin/bash

host=$1

scp -r "$host:/var/log/discern/json-data/" .
scp -r "$host:/var/log/discern/rotated-logs/" .

