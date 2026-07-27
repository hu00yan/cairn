--------------------------- MODULE publish_recovery ---------------------------
EXTENDS Naturals, FiniteSets, TLC

\* Protocol model only: this is not a refinement proof of the Rust code.
\* SQLite catalog state is the visibility authority. A durable DAG candidate
\* alone is never reader-visible.

CONSTANTS Operations, Versions, Readers, Epochs, MaxEpoch, NoVersion,
          InitialVisible, InitialDurable

Phases == {"idle", "prepared", "commit_durable", "published", "aborted"}

VARIABLES catalogVisible, durableCandidates, reclaimed, phase, candidate,
          result, fenceEpoch, ownerEpoch, processOpen, readerPin

vars == <<catalogVisible, durableCandidates, reclaimed, phase, candidate,
          result, fenceEpoch, ownerEpoch, processOpen, readerPin>>

RootSet ==
    catalogVisible
    \cup {readerPin[r] : r \in Readers}
    \cup {candidate[o] : o \in {x \in Operations :
              phase[x] \in {"prepared", "commit_durable"}}}

RequestedVersion ==
    [o \in Operations |-> CHOOSE v \in Versions: TRUE]

Init ==
    /\ InitialVisible \subseteq InitialDurable
    /\ catalogVisible = InitialVisible
    /\ durableCandidates = InitialDurable
    /\ reclaimed = {}
    /\ phase = [o \in Operations |-> "idle"]
    /\ candidate = [o \in Operations |-> NoVersion]
    /\ result = [o \in Operations |-> NoVersion]
    /\ fenceEpoch = 0
    /\ ownerEpoch = [o \in Operations |-> 0]
    /\ processOpen = TRUE
    /\ readerPin = [r \in Readers |-> NoVersion]

\* T1 atomically records the operation and preparing intent in SQLite.
T1(o) ==
    /\ processOpen
    /\ phase[o] = "idle"
    /\ candidate' = [candidate EXCEPT ![o] = RequestedVersion[o]]
    /\ phase' = [phase EXCEPT ![o] = "prepared"]
    /\ ownerEpoch' = [ownerEpoch EXCEPT ![o] = fenceEpoch]
    /\ UNCHANGED <<catalogVisible, durableCandidates, reclaimed, result, fenceEpoch,
                   processOpen, readerPin>>

\* A retry of the same operation/request changes no durable fact.
IdempotentRetry(o) ==
    /\ processOpen
    /\ phase[o] \in {"prepared", "commit_durable", "published"}
    /\ candidate[o] = RequestedVersion[o]
    /\ UNCHANGED vars

DagAppendFlush(o) ==
    /\ processOpen
    /\ phase[o] = "prepared"
    /\ ownerEpoch[o] = fenceEpoch
    /\ candidate[o] \notin reclaimed
    /\ durableCandidates' = durableCandidates \cup {candidate[o]}
    /\ UNCHANGED <<catalogVisible, reclaimed, phase, candidate, result, fenceEpoch,
                   ownerEpoch, processOpen, readerPin>>

\* T2 records the durable candidate but does not publish it.
T2(o) ==
    /\ processOpen
    /\ phase[o] = "prepared"
    /\ candidate[o] \in durableCandidates
    /\ candidate[o] \notin reclaimed
    /\ ownerEpoch[o] = fenceEpoch
    /\ phase' = [phase EXCEPT ![o] = "commit_durable"]
    /\ UNCHANGED <<catalogVisible, durableCandidates, reclaimed, candidate, result,
                   fenceEpoch, ownerEpoch, processOpen, readerPin>>

\* T3 is the only transition that changes catalog visibility.
T3(o) ==
    /\ processOpen
    /\ phase[o] = "commit_durable"
    /\ candidate[o] \in durableCandidates
    /\ candidate[o] \notin reclaimed
    /\ ownerEpoch[o] = fenceEpoch
    /\ catalogVisible' = catalogVisible \cup {candidate[o]}
    /\ phase' = [phase EXCEPT ![o] = "published"]
    /\ result' = [result EXCEPT ![o] = candidate[o]]
    /\ UNCHANGED <<durableCandidates, reclaimed, candidate, fenceEpoch, ownerEpoch,
                   processOpen, readerPin>>

\* A failed/aborted publish removes the pending candidate from the root set;
\* a later Reclaim may then retire its durable DAG records.
Abort(o) ==
    /\ processOpen
    /\ phase[o] \in {"prepared", "commit_durable"}
    /\ phase' = [phase EXCEPT ![o] = "aborted"]
    /\ UNCHANGED <<catalogVisible, durableCandidates, reclaimed, candidate, result,
                   fenceEpoch, ownerEpoch, processOpen, readerPin>>

Crash ==
    /\ processOpen
    /\ processOpen' = FALSE
    /\ readerPin' = [r \in Readers |-> NoVersion]
    /\ UNCHANGED <<catalogVisible, durableCandidates, reclaimed, phase, candidate, result,
                   fenceEpoch, ownerEpoch>>

Reopen ==
    /\ ~processOpen
    /\ processOpen' = TRUE
    /\ fenceEpoch' = IF fenceEpoch < MaxEpoch
                        THEN fenceEpoch + 1
                        ELSE fenceEpoch
    /\ UNCHANGED <<catalogVisible, durableCandidates, reclaimed, phase, candidate, result,
                   ownerEpoch, readerPin>>

ClaimRecovery(o) ==
    /\ processOpen
    /\ phase[o] \in {"prepared", "commit_durable"}
    /\ ownerEpoch[o] < fenceEpoch
    /\ ownerEpoch' = [ownerEpoch EXCEPT ![o] = fenceEpoch]
    /\ UNCHANGED <<catalogVisible, durableCandidates, reclaimed, phase, candidate, result,
                   fenceEpoch, processOpen, readerPin>>

PinReader(r, v) ==
    /\ processOpen
    /\ v \in catalogVisible
    /\ readerPin' = [readerPin EXCEPT ![r] = v]
    /\ UNCHANGED <<catalogVisible, durableCandidates, reclaimed, phase, candidate, result,
                   fenceEpoch, ownerEpoch, processOpen>>

UnpinReader(r) ==
    /\ readerPin[r] # NoVersion
    /\ readerPin' = [readerPin EXCEPT ![r] = NoVersion]
    /\ UNCHANGED <<catalogVisible, durableCandidates, reclaimed, phase, candidate, result,
                   fenceEpoch, ownerEpoch, processOpen>>

Reclaim(v) ==
    /\ processOpen
    /\ v \in durableCandidates
    /\ v \notin RootSet
    /\ reclaimed' = reclaimed \cup {v}
    /\ UNCHANGED <<catalogVisible, durableCandidates, phase, candidate, result,
                   fenceEpoch, ownerEpoch, processOpen, readerPin>>

Next ==
    \/ \E o \in Operations:
         T1(o) \/ IdempotentRetry(o) \/ DagAppendFlush(o) \/ T2(o) \/ T3(o)
           \/ Abort(o) \/ ClaimRecovery(o)
    \/ \E r \in Readers, v \in Versions: PinReader(r, v)
    \/ \E r \in Readers: UnpinReader(r)
    \/ \E v \in Versions: Reclaim(v)
    \/ Crash
    \/ Reopen

TypeOK ==
    /\ catalogVisible \subseteq Versions
    /\ durableCandidates \subseteq Versions
    /\ reclaimed \subseteq Versions
    /\ phase \in [Operations -> Phases]
    /\ candidate \in [Operations -> Versions \cup {NoVersion}]
    /\ result \in [Operations -> Versions \cup {NoVersion}]
    /\ fenceEpoch \in Epochs
    /\ ownerEpoch \in [Operations -> Epochs]
    /\ processOpen \in BOOLEAN
    /\ readerPin \in [Readers -> Versions \cup {NoVersion}]

CatalogVisibilityIsDurable == catalogVisible \subseteq durableCandidates

NoVisibilityBeforeT3 ==
    \A o \in Operations:
        phase[o] \in {"prepared", "commit_durable"}
          => candidate[o] \notin catalogVisible

PublishedResultIsStable ==
    \A o \in Operations:
        phase[o] = "published"
          => result[o] = candidate[o] /\ candidate[o] \in catalogVisible

FenceSafety == \A o \in Operations: ownerEpoch[o] <= fenceEpoch

PinnedReadersAreRooted ==
    \A r \in Readers:
        readerPin[r] # NoVersion => readerPin[r] \in RootSet

RecoveryCandidatesAreRooted ==
    \A o \in Operations:
        phase[o] \in {"prepared", "commit_durable"} => candidate[o] \in RootSet

NoRootIsReclaimed == reclaimed \cap RootSet = {}

Safety ==
    TypeOK
    /\ CatalogVisibilityIsDurable
    /\ NoVisibilityBeforeT3
    /\ PublishedResultIsStable
    /\ FenceSafety
    /\ PinnedReadersAreRooted
    /\ RecoveryCandidatesAreRooted
    /\ NoRootIsReclaimed

\* Liveness/recovery assumptions:
\* 1. crashes eventually stop long enough for reopen and recovery;
\* 2. SQLite and flushed DAG records remain readable;
\* 3. enabled recovery, append/flush, T2, and T3 actions are scheduled;
\* 4. clients retry an operation with the same RequestedVersion.
LivenessAssumptions ==
    /\ <>[]processOpen
    /\ WF_vars(Reopen)
    /\ \A o \in Operations:
         WF_vars(ClaimRecovery(o))
         /\ WF_vars(DagAppendFlush(o))
         /\ WF_vars(T2(o))
         /\ WF_vars(T3(o))

Spec == Init /\ [][Next]_vars /\ LivenessAssumptions

RecoveryCompletes ==
    \A o \in Operations:
        phase[o] \in {"prepared", "commit_durable"}
          ~> phase[o] \in {"published", "aborted"}

=============================================================================
