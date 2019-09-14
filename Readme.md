# miniTSDB
miniTSDB is a lightweight time series database.

## Goals
- minimal disk wear
- automatic downsampling
- automatic selection of retention policies for queries
- open data format

This combination makes it perfect for collecting data on low-end devices such as the Raspberry Pi.

## Project Status
miniTSDB works well in it's current form and runs 24/7 on my Raspberry Pi.
There are, however, a lot of tasks that still have to be taken on:

- tests
- documentation
- improve data source plugin for Grafana
- more data sources

Lacking time, I'll only work on issues that require immediate attention, such as bugs that lead to crashes or data loss.