

add a fetch ahead thread which 

i still dont know how often to make the fetch ahead thread perform the operations, it must always keep running tbh, basically, either every millisecond or when the shit in pipe is already too much



## changes in iohandler
add a batch read endpoint powered by io_uring, fuck macos support

## pre-fetcher
takes in shit requests with a crossbeam channel, basically, either every millisecond or when the shit in pipe is already too much and send it to iohandler itself and insert in compressed cache
