#/usr/bin/env bash
echo "Starting"
TIC_COUNT=10
cur_count=0
while true; do
    cur_count=$((cur_count + 1))
    if [ "$cur_count" -ge "$TIC_COUNT" ]; then
        break
    fi
    echo "$(($(date +%s) * 1000 + $(date +%-N) / 1000000))"
    sleep 1
done

echo "Complete"
