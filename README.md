# rngcache

A caching tool for python. Useful for training models with machine learning algorithms.

Random files will be loaded from a given directory into RAM (/dev/shm/ by default). 
The cache is continuously refreshing the files with random ones.

## Usage

```python
from rngcache import RandomFileCache

# 200MB cache
cache = RandomFileCache("path/to/files/", cache_size=200) 
cache.start()
# ...
with cache.get_random_file() as file_path:
    # load file ...
```



Linux only.
