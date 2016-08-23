"""
from rngcache import RandomFileCache
cache = RandomFileCache(rootdir, cache_size=200) # 200MB cache
cache.start()
# ...
with cache.get_random_file() as file_path:
    # load file ... cPickle.load(open(file_path,"rb")) ...
"""

import signal
import ctypes
import shutil
import random
from time import time, sleep
import multiprocessing as mp
from glob import glob


class RandomFileCache(object):
    def __init__(self,
                 root_dir,
                 cache_size, #in MB
                 cache_dir="/dev/shm/",
                 max_files=10000
                 ):
        if not root_dir.endswith("/"): root_dir += "/"
        self.cache_dir = cache_dir
        self.root_dir = root_dir
        self.max_MB = cache_size
        self.max_files = max_files
        self.init = False

    def start(self):
        self.is_terminated = mp.Value(ctypes.c_bool, False)
        self.cache_ready = mp.Event()

        self.files = glob(self.root_dir+"*")
        manager = mp.Manager()
        self.cache = manager.list()

        self.cache_dir += ""+str(time())+"/"
        os.mkdir(self.cache_dir)

        # locks can't be made on the fly (not pickable), so allocate a bunch of them
        self.locks = [mp.Lock() for _ in range(self.max_files)]
        self.lock_idxs = range(self.max_files) # lock references

        self.job = mp.Process(target=self.cache_process)
        self.job.daemon = True
        self.job.start()

        signal.signal(signal.SIGINT, self.terminate)
        signal.signal(signal.SIGTERM, self.terminate)

        self.init = True

    def get_random_file(self):
        self.cache_ready.wait()
        while True:
            entry = random.choice(self.cache)
            lock = self.locks[entry["lock"]]
            if not lock.acquire(False): continue # is locked, go to next
            cached_file = CachedFile(self.cache_dir + entry["key"])
            cached_file.set_lock(lock)
            return cached_file # extension of string

    def cache_process(self):
        self.size = 0
        while not self.is_terminated.value:
            file = random.choice(self.files)
            file_size = os.path.getsize(file) >> 20

            # if not enough space, remove a random entry
            if self.size + file_size > self.max_MB:
                self.cache_ready.set()
                self.remove_entry()
                continue

            key = os.path.basename(file)

            # don't allow duplicates
            if self.contains(key): continue

            shutil.copyfile(file, self.cache_dir+key)
            self.size += file_size
            self.cache.append( {"key":key, "lock":self.lock_idxs.pop(0)} )

    def remove_entry(self):
        if len(self.cache) == 0: return
        while True:
            for i, entry in enumerate(self.cache):

                # lock entry
                lock_idx = entry["lock"]
                lock = self.locks[lock_idx]
                if not lock.acquire(False): continue

                file = self.cache_dir+entry["key"]
                self.size -= os.path.getsize(file) >> 20

                # remove file
                os.remove(file)
                del self.cache[i]
                self.lock_idxs.append(lock_idx)
                lock.release()
                return
            sleep(0.01)

    def contains(self, key):
        for entry in self.cache:
            if entry["key"] == key: return True
        return False

    def terminate(self, sig=None, frame=None):
        if not self.init or self.is_terminated.value: return
        self.is_terminated.value = True
        for lock in self.locks:
            try: lock.release()
            except ValueError: pass
        if os.path.exists(self.cache_dir): shutil.rmtree(self.cache_dir)
        self.job.join()


class CachedFile(str):
    def set_lock(self, lock): self.lock = lock
    def __enter__(self): return self
    def __exit__(self, type, value, traceback): self.lock.release()


if __name__ == '__main__':
    import yaml
    import os
    from moviepy.editor import VideoFileClip
    os.chdir("..")
    with open("paths.yaml", "r") as f: PATH = yaml.load(f)
    cache = RandomFileCache(PATH["preproc"] + "25fps_crf28", cache_size=200)
    cache.start()
    print len(cache.files)

    for i in range(10):
        with cache.get_random_file() as file:
            print file
            print cache.cache
            clip = VideoFileClip(file)
            clip.preview()

            # for l in cache.locks:
            #     if not l.acquire(False):
            #         count += 1
            #         continue
            #     l.release()
            # print count, "locked"

    cache.job.join()
    cache.terminate()