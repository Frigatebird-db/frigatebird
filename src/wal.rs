// this is our crazy WAL boi

/*
[BlockAllocator] -> just hands over blocks/block metas ?? idk

we need a reader too btw

so, we kinda need to keep track of... Mmaped blocks 
are we mmaping block or whole ass files ?? I dont want to make too many actual mmap copies over same file which would be shared by various blocks owned by various columns btw

so we can just have something like, I mean, it might be cheaper... oh, shit, yeah, no: 


okay, so whenever a column gets a new block allocated to them, we can just add it to their block chain

M[col] -> {
            chain: {(mmap object,relative_block number)-()-()-()...}
            last_checkpoint
          }


..


okay, how to implement a reader over this, we kinda also need some sort of... checkpoint over this..
idk, lets keep it simple for now lol

so we just want to read it from a checkpoint, 

now, the thing is, what the hell do you even mark in a fucking checkpoint lol, like, mmap in the chain is just a whole darn file mapped in virtual memory
hmm, you can have it something like: (chain_idx,offset_in_file) LMAOOOOO, hmm, seems pretty... straightfoward to implement too
wait, we need something more, we need the relative block number too along with each mmap in chain so that we can know which fucking block

note that the data once written in the block is immutable

we can parallelize as much as possible as long as we keep throwing cores at it

we really need a scheduler man, LMAO
*/