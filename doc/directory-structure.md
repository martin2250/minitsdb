```
database
|-power.main
  |-series.yaml
  |-1
    |-153450000.db
    |-153460000.db
  |-60
    |-xxx.db
```

`power.main/series.yaml`
```yaml
tags:
  name: power # folder name does not matter, name is derived from tags
  loc: main

flushdelay: 5m # automatically flush when the oldest data stored only in RAM is 5 minutes old
buffer: 500     # buffer 500 points before trying to write a block
reusemax: 3800  # reuse (append new data to) last block in file if fewer bytes are used in that block

buckets:        # first bucket sets time resolution -> here 1s
  - factor: 1   # creates folder '1'
  - factor: 60  # creates folder '60'
  - factor: 60  # creates folder '3600'

columns:
  - decimals: 3
    tags:
      name: frequency # name is mandatory

  - decimals: 1 # todo: switch to using watts as unit
    tags:
      name: power
    duplicate:  # duplicate creates multiple copies of this column with additional tags
      - phase: A
      - phase: B
      - phase: C
    
  - decimals: 2
    tags:
      name: voltage
    duplicate:
      - phase: A
      - phase: B
      - phase: C
    
  - decimals: 4
    tags:
      name: current
    duplicate:
      - phase: A
      - phase: B
      - phase: C
      - phase: T
    
  - decimals: 4
    tags:
      name: energy
    duplicate:
      - phase: A
      - phase: B
      - phase: C
      - phase: T
    
```

`sensor.weatherstation/series.yaml`
```yaml
tags:
  name: sensor # folder name does not matter, name is derived from tags
  loc: weatherstation

flushdelay: 1h # automatically flush when the oldest data stored only in RAM is 5 minutes old
buffer: 500     # buffer 500 points before trying to write a block
reusemax: 3800  # reuse (append new data to) last block in file if fewer bytes are used in that block

buckets:        # first bucket sets time resolution -> here 1m
  - factor: 60  # creates folder '60'
  - factor: 60  # creates folder '3600'

columns:
  - decimals: 2
    tags:
      name: pressure
      pos: outside

  - decimals: 2
    tags:
      name: temperature
    duplicate:
      - pos: outside
      - pos: inside
    
  - decimals: 2
    tags:
      name: voltage
      pos: battery
    
  - decimals: 4
    tags:
      name: current
      pos: battery
    
  - decimals: 1
    tags:
      name: rssi
    
```