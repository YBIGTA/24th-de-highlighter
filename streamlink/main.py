import streamlink, time

URL = "https://www.youtube.com/watch?v=-rX9VpMLxOk"
TIME = 5
BUF_SIZE = 1024

streams = streamlink.streams(URL)
print("Streams found: ", *streams.keys())

fd = streams['480p'].open()

for i in range(3):
    print(f"Video #{i+1}...")
    fname = f"video-{i}.ts"
    with open(fname, "wb") as video_binary:
        now = time.time()

        while time.time() - now < TIME:
            video_binary.write(fd.read(BUF_SIZE))
