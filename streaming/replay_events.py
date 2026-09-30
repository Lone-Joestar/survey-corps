import argparse,csv,json,time

from confluent_kafka import Producer

def clean(r):
    """
    Writing this here so i remember later what this function is for , turn one csv row(all strings)
    into a dict with proper types
    CSV has no types and no nulls: an empty cell is just ""
    We convert ""-> None (becomes JSON null) and numbers to int/float
    """

    def num(x,cast):
        return cast(float(x)) if x != "" else None

    return {
        "event_id": int(r["event_id"]),
        "session_id": int(r["session_id"]),
        "timestamp": r["timestamp"],          # keep as string; Flink parses it later
        "event_type": r["event_type"],
        "product_id": num(r["product_id"], int),
        "qty": num(r["qty"], int),
        "cart_size": num(r["cart_size"], int),
        "payment": r["payment"] or None,      # "" is falsy, so it becomes None
        "discount_pct": num(r["discount_pct"], float),
        "amount_usd": num(r["amount_usd"], float),
    }

def main():
    # Command-line options, so you can run: python replay_events.py --limit 1000
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default="events.csv")
    ap.add_argument("--topic", default="events")
    ap.add_argument("--bootstrap", default="localhost:9092")  # Kafka address from your Mac
    ap.add_argument("--rate", type=float, default=0)   # events/sec; 0 = as fast as possible
    ap.add_argument("--limit", type=int, default=0)    # 0 = send everything
    args = ap.parse_args()

    with open(args.file,newline="") as f:
        rows=list(csv.DictReader(f))

    # here i sorted the file by time as it is sorted by session id
    #basically sorted them in the order they happenned 
    rows.sort(key=lambda r:(r["timestamp"], int(r["event_id"])))

    if args.limit:
        rows=rows[:args.limit]
    print(f"Replaying {len(rows)} events, {rows[0]['timestamp']} -> {rows[-1]['timestamp']}")
    #Create the Kafka producer (the connection to the broker)
    producer = Producer({"bootstrap.servers": args.bootstrap})
    errors=0

    #kafka works asynchronously, this callback fires later once per message
    def on_delivery(err,msg):
        nonlocal errors
        if err:
            errors+=1
            print("delivery failed",err)

    for i,r in enumerate(rows,1):
        while True:
            try:
                producer.produce(
                    args.topic,
                    key=r["session_id"],
                    value=json.dumps(clean(r)),
                    callback=on_delivery
                )
                break
            except BufferError:
                #local send queue is full,wait for it to drain then retry
                producer.poll(0.5)

        producer.poll(0)
        if args.rate: 
            time.sleep(1/args.rate) #throtting to simulate a live stream
        if i % 10000 == 0:
            print(f"sent {i}")

    #block until every queued message is delivered
    producer.flush()
    print(f"Done .{len(rows)} sent,{errors}errors")

if __name__=="__main__":
    main()