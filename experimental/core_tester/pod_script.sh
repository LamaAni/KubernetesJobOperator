#/usr/bin/env bash
echo "Starting"
TIC_COUNT=60
cur_count=0
while true; do
    cur_count=$((cur_count + 1))
    if [ "$TIC_COUNT" -ge "$cur_count" ]; then
        break
    fi
    echo "$(($(date +%s) * 1000 + $(date +%-N) / 1000000))"
    sleep 1
done

echo "Complete"
exit 0
