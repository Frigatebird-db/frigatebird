

add a fetch ahead thread which 

i still dont know how often to make the fetch ahead thread perform the operations, it must always keep running tbh, basically, either every millisecond or when the shit in pipe is already too much



## changes in iohandler
add a batch read endpoint powered by io_uring, fuck macos support

## pre-fetcher
takes in shit requests with a crossbeam channel, basically, either every millisecond or when the shit in pipe is already too much and send it to iohandler itself and insert in compressed cache


we need to ensure that because we are using O_DIRECT to read stuff with io_uring, our offsets(that we put into io_uring requests to read) need to be aligned i.e. multiple of 4kb, but ofc in reality they wont always be a multiple of that, our buffer size is fine because its always a multiple of 1mb, how would you fix this issue, without making any changes yet, tell me about it first