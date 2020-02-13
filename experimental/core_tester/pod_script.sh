#/usr/bin/env bash
echo "Starting"
TIC_COUNT=0
cur_count=0
while true; do
    if [ "$cur_count" -ge "$TIC_COUNT" ]; then
        break
    fi
    date
    sleep 1
    cur_count=$((cur_count + 1))
done

echo "Complete"
