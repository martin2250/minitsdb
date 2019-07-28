# MiniTSDB

## Threading Model
All Database related operations happen in the main program loop.
Interaction with outside clients is handled by goroutines, communication with the main loop happens through FIFOs.

#### Inserting Points:
Points can come from various sources like HTTP or raw TCP listeners.
All sources implement the PointSource interface, which exposes a GetPoint method.
This is done to allow extracting the listeners (and buffers) to a separate process, continuing to collect points even when the main database process is not running.
#### Queries:
Queries are accepted by a http endpoint, which is handled by goroutines.

When a query is received, all operations that do not depend on the data are executed on this goroutine:
- check if the series exists
- adjust time step to fit bucket time steps

The query object is then stored in a FIFO and later executed in the main loop.
The http writer is passed to the main loop.

#### Main Loop Operations
 - Read new points from the point source and insert all available points (one by one, with timeout to keep queries responsive)
 - Check RAM buffers of all series and encode values to disk if necessary
 - Check buckets and downsample to larger buckets if possible
 - 

### Downsampling steps
- 1s/1week -> 604800 points
- 30s/30weeks -> 604800 points
- 15m/900weeks -> 604800 points