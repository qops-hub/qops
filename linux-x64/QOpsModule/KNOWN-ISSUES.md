# QOps — Known Issues

## Portal (beta): Dry Run
Dry Run: the misleading toggle has been temporarily removed; a real plan-only Dry Run is tracked in QOPS-196.

## Rollback 3.x -> 2.x: `Unexpected character encountered while parsing value: A`

QOps 3.x writes its config as YAML by default; QOps 2.x parses JSON only. After a rollback to 2.x, every
command fails with a Newtonsoft parse error that names neither the file nor the format, which sends you
looking at the network instead of at a local file.

Remove or rename the configs and 2.x will create a fresh one:

    <current directory and its parents>\.qopsconfig
    %USERPROFILE%\qopsconfig\.qopsconfig        (keep license.key in that folder)

Rolling back is not a supported path - this note exists because the error message does not point at the
cause.
