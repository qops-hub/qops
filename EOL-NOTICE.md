# QOps 2.x — End of Support

**This channel is frozen. QOps 2.x reaches end of support on 2027-02-01.**

## What this means

This branch (`main`) is the **2.x distribution channel**. It is no longer developed: it receives no
features, and no fixes other than anything judged critical before the date below.

| Date | What happens |
|------|--------------|
| 2026-08-01 | End-of-support announced, six months' notice. The channel keeps working. |
| 2027-02-01 | **End of support.** This channel closes and stops serving updates. |

Nothing stops working on 2027-02-01 — an installed 2.x keeps running. What ends is updates, fixes and
support for it.

## What to do

Move to **3.x**:

```powershell
QOps-Update -Force
```

3.x is the current line and is where all development happens. It is distributed from the `main-3.x`
branch of this repository.

If you cannot upgrade yet, tell us why — a blocker that is not on our radar is the one thing that can
still change these dates.

## Current contents of this channel

The last 2.x build published here is **2.2.6**. It is unchanged in behaviour from 2.2.5 except that it
prints this end-of-support notice before and after each operation.

| Directory | Runtime |
|-----------|---------|
| `x64/` | Windows PowerShell 5.1 (.NET Framework) |
| `x64-net7.0/` | PowerShell 7+ on Windows (.NET 7) |
| `linux-x64/` | PowerShell 7+ on Linux (.NET 7) |

3.x ships a `.NET 8` build instead; that is one of the reasons the upgrade is worth doing rather than
deferring.

## More

Full notice, rationale and the upgrade path:
<https://qops.datalabsua.com/documentation/qops-2x-end-of-support/>

Questions, or an upgrade blocker: raise it with your DatalabsUa contact.

---

*QOps is developed by DatalabsUa. This file marks the 2.x channel as end-of-life; it does not change any
build already published here.*
