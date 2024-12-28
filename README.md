# Creedmoor

# Don't use this.

# But if you really do

You could've written this yourself. I'm not sure it even makes sense to publish it, but maybe someone else needed this too and can help me polish it up.

# What is it?

I wanted dual-layer memory + disk caching and I didn't really want to use mmap and juggle raw pointers & disk offsets. I also wanted something that could gracefully grow and shrink.

In addition, I wanted something that would let me set size limits for the memory _and_ the disk cache separately in bytes. Most caching libraries only let you set an item count and while my items happen to be consistent in size in my use-case, I'll sleep better doing it that way.

[sled](https://sled.rs/) is an embedded database, akin to `acid-state` for Haskell but written in Rust and a little lower-level.

One feature `sled` has that is convenient for my use-case is that it has a page cache. A page cache that you can set a cache capacity limit for. So between that and the fact that `sled` is already handling disk-persistence for me, I'm getting in-memory caching and on-disk caching. All that leaves is disk usage/database size. So we go ahead and enforce that ourselves in `sled`. Also `sled` gets us compression and other niceties like transactions.

Neither the memory usage nor disk usage are going to be exact because there's going to be some overhead, but it should work well enough for most use-cases.
