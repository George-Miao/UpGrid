# UpGrid Compatibility Patch

This is Cyper `0.9.0` from crates.io. UpGrid keeps it local because Compio
`master` moved `ensure_init` to the `IoBufMutExt` trait after the Cyper release.

The only source change imports that trait in `src/stream.rs`. Remove this copy
after Cyper publishes the corresponding Compio-master compatibility update.
