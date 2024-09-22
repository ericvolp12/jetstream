i=0; websocat "ws://localhost:6008/subscribe?cursor=0" | while IFS= read -r line; do
  echo "$line" > "training/$i.json"
  i=$((i+1))
  if [ "$i" -ge 100000 ]; then
    break
  fi
done
