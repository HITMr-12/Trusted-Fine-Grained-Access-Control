import concurrent.futures, pathlib, sys, urllib.request, time
url, target = sys.argv[1:]
target = pathlib.Path(target)
def get(start, end):
    for attempt in range(4):
        try:
            req = urllib.request.Request(url, headers={'Range': f'bytes={start}-{end}'})
            with urllib.request.urlopen(req, timeout=90) as response:
                assert response.status == 206, response.status
                data = response.read()
                assert len(data) == end-start+1
                return data
        except Exception:
            if attempt == 3: raise
            time.sleep(1)
with urllib.request.urlopen(urllib.request.Request(url, headers={'Range': 'bytes=0-0'}), timeout=30) as response:
    size = int(response.headers['Content-Range'].split('/')[-1])
chunk = 4*1024*1024
parts = target.with_suffix(target.suffix+'.parts')
parts.mkdir(exist_ok=True)
def download(i):
    start, end = i*chunk, min(size,(i+1)*chunk)-1
    p = parts/str(i)
    if not p.exists() or p.stat().st_size != end-start+1:
        p.write_bytes(get(start,end))
    print(f'chunk {i+1}/{(size+chunk-1)//chunk}', flush=True)
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
    list(pool.map(download,range((size+chunk-1)//chunk)))
with target.open('wb') as out:
    for i in range((size+chunk-1)//chunk): out.write((parts/str(i)).read_bytes())
assert target.stat().st_size == size
print('COMPLETE',target,size,flush=True)
