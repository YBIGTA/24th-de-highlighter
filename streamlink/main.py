import streamlink, datetime, sys, time

# URL = sys.argv[1]
# BUF_SIZE = int(sys.argv[2])
# TIME = int(sys.argv[3])

URL = "https://www.youtube.com/watch?v=Z2MR73ELLkE"
BUF_SIZE = 4096
TIME = 300

streams = streamlink.streams(URL)
print("Streams found: ", *streams.keys())

fd = streams['best'].open()

now = time.time()
lines = 0
total_data = 0 

with open("vid.ts", "wb") as video_file:
    while time.time() - now < TIME:
        data = fd.read(BUF_SIZE)
        lines += 1
        total_data += len(data)

        video_file.write(data)

        # TODO: Implement Kafka producer
        if len(data) < BUF_SIZE:
            print(f"{datetime.datetime.now()} | {lines} {len(data)}/{BUF_SIZE}")

print(f"Utilization: {total_data / (lines * BUF_SIZE) :.4f}")
