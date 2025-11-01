and I think we can do it better, especially updating the prefix sum part, note that since every single time we add/remove something from a page, we need to do +1 till the very end since that page to the end of the prefix sum array, this is something that we can parallelize with SIMD, its just a range op, basically something like: "for i to n, do +x on each element"

wdyt, tell me about it
