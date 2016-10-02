# superpack
Experiments with go "errgroup" package, multistream gzip and sparse files...

```
$ time tar czf /tmp/output-baseline.tgz /usr/lib/x86_64-linux-gnu/

real    1m20.265s
user    1m19.848s
sys     0m1.776s
```

```
$ time spack /tmp/output-cold.tgz /usr/lib/x86_64-linux-gnu/ # cold cache

real    0m24.339s
user    1m25.144s
sys     0m5.472s
```

```
$ time spack /tmp/output-hot.tgz /usr/lib/x86_64-linux-gnu/ # hot cache

real    0m2.824s
user    0m5.200s
sys     0m1.524s
```

```
$ du -bh /tmp/output*
483M    /tmp/output-baseline.tgz
570M    /tmp/output-cold.tgz
570M    /tmp/output-hot.tgz
```
* we are using '-b' == "--apparent-size --block-size=1"

```
$ grep -c processor /proc/cpuinfo 
4
```

