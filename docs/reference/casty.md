# casty

Everything `casty.__all__` exports. The observer events and counts are defined in
[`casty.observer`](observer.md), and `Collections` in [`casty.collections`](collections.md); `casty` re-exports
them under the same names.

## Actor types

::: casty.actor

::: casty.Actor

::: casty.DefaultedActor

::: casty.ActorDefinition

::: casty.Body

::: casty.Write

::: casty.OnFull

::: casty.Durable

::: casty.Backoff

## Bodies

::: casty.Context

::: casty.State

::: casty.Schedule

::: casty.Ref

## Systems

::: casty.ActorSystem

::: casty.Client

::: casty.Runtime

::: casty.System

::: casty.NodeId

::: casty.Member

::: casty.Placement

## Clusters

::: casty.Cluster

::: casty.TLS

::: casty.Compression

::: casty.Limits

::: casty.Overlay

## Stored values

::: casty.Store

::: casty.Opaque

## Errors

::: casty.SchemaError

::: casty.NotStarted

::: casty.UnknownActor

::: casty.MailboxFull

::: casty.Unavailable

::: casty.Refused

::: casty.MessageTooLarge

::: casty.ReentrancyError

::: casty.ActorFailed

## Re-exported

- [`Collections`][casty.collections.Collections], from `casty.collections`.
- From `casty.observer`: [`Observer`][casty.observer.Observer], [`LoggingObserver`][casty.observer.LoggingObserver],
  [`Event`][casty.observer.Event], [`MemberChanged`][casty.observer.MemberChanged],
  [`ActivationStarted`][casty.observer.ActivationStarted], [`ActivationEnded`][casty.observer.ActivationEnded],
  [`ActivationFailed`][casty.observer.ActivationFailed], [`WriteFailed`][casty.observer.WriteFailed],
  [`HandoffStarted`][casty.observer.HandoffStarted], [`HandoffEnded`][casty.observer.HandoffEnded],
  [`ConnectionLost`][casty.observer.ConnectionLost], [`MessageDropped`][casty.observer.MessageDropped],
  [`Stats`][casty.observer.Stats], [`ActorStats`][casty.observer.ActorStats],
  [`Activation`][casty.observer.Activation].
