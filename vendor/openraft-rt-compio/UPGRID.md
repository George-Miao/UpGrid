# UpGrid Compatibility Patch

This crate is copied from `George-Miao/openraft` commit
`c4a47d1a` (`rt-compio`). UpGrid keeps it local because that adapter does not
yet compile with Compio `master`: Compio now returns a typed `JoinError` rather
than a boxed panic payload.

The local change boxes that error in `CompioJoinHandle::poll`, matching the
implementation published by newer `openraft-rt-compio` releases. Remove this
copy once UpGrid migrates to an OpenRaft revision with compatible Compio runtime
support.
