"""Saga pattern: Distributed transactions with compensating actions.

Demonstrates:
- Orchestrator manages transaction steps
- Steps: reserve hotel -> reserve flight -> charge payment
- Compensating actions on failure (rollback)
- Shows both successful flow and failure with compensation

Run with:
    uv run python examples/patterns/04-saga.py
"""

import asyncio
from dataclasses import dataclass
from enum import Enum

from casty import actor, ActorSystem, Mailbox, LocalActorRef


class BookingStatus(Enum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass
class ReserveHotel:
    booking_id: str
    hotel: str
    nights: int


@dataclass
class CancelHotelReservation:
    booking_id: str


@dataclass
class HotelReserved:
    booking_id: str
    confirmation: str


@dataclass
class HotelCancelled:
    booking_id: str


@dataclass
class HotelFailed:
    booking_id: str
    reason: str


@dataclass
class ReserveFlight:
    booking_id: str
    flight: str


@dataclass
class CancelFlightReservation:
    booking_id: str


@dataclass
class FlightReserved:
    booking_id: str
    confirmation: str


@dataclass
class FlightCancelled:
    booking_id: str


@dataclass
class FlightFailed:
    booking_id: str
    reason: str


@dataclass
class ChargePayment:
    booking_id: str
    amount: float


@dataclass
class RefundPayment:
    booking_id: str


@dataclass
class PaymentCharged:
    booking_id: str
    transaction_id: str


@dataclass
class PaymentRefunded:
    booking_id: str


@dataclass
class PaymentFailed:
    booking_id: str
    reason: str


@dataclass
class BookTrip:
    booking_id: str
    hotel: str
    nights: int
    flight: str
    total_amount: float


@dataclass
class BookingResult:
    booking_id: str
    success: bool
    message: str
    hotel_confirmation: str | None = None
    flight_confirmation: str | None = None
    payment_transaction: str | None = None


@dataclass
class SagaState:
    booking: BookTrip
    sender: LocalActorRef | None
    hotel_confirmation: str | None = None
    flight_confirmation: str | None = None
    payment_transaction: str | None = None
    step: str = "hotel"
    compensating: bool = False


HotelMsg = ReserveHotel | CancelHotelReservation
FlightMsg = ReserveFlight | CancelFlightReservation
PaymentMsg = ChargePayment | RefundPayment

SagaResponse = (
    HotelReserved | HotelFailed | HotelCancelled |
    FlightReserved | FlightFailed | FlightCancelled |
    PaymentCharged | PaymentFailed | PaymentRefunded
)


@actor
async def hotel_service(should_fail: bool = False, *, mailbox: Mailbox[HotelMsg]):
    reservations: dict[str, str] = {}

    async for msg, ctx in mailbox:
        match msg:
            case ReserveHotel(booking_id, hotel, nights):
                await asyncio.sleep(0.1)
                if should_fail:
                    print(f"  [Hotel] Reservation FAILED for {hotel}")
                    await ctx.reply(HotelFailed(booking_id, "No rooms available"))
                else:
                    confirmation = f"HOTEL-{booking_id[:8]}"
                    reservations[booking_id] = confirmation
                    print(f"  [Hotel] Reserved {hotel} for {nights} nights: {confirmation}")
                    await ctx.reply(HotelReserved(booking_id, confirmation))

            case CancelHotelReservation(booking_id):
                await asyncio.sleep(0.05)
                conf = reservations.pop(booking_id, None)
                print(f"  [Hotel] Cancelled reservation: {conf or 'N/A'}")
                await ctx.reply(HotelCancelled(booking_id))


@actor
async def flight_service(should_fail: bool = False, *, mailbox: Mailbox[FlightMsg]):
    reservations: dict[str, str] = {}

    async for msg, ctx in mailbox:
        match msg:
            case ReserveFlight(booking_id, flight):
                await asyncio.sleep(0.1)
                if should_fail:
                    print(f"  [Flight] Reservation FAILED for {flight}")
                    await ctx.reply(FlightFailed(booking_id, "Flight fully booked"))
                else:
                    confirmation = f"FLIGHT-{booking_id[:8]}"
                    reservations[booking_id] = confirmation
                    print(f"  [Flight] Reserved {flight}: {confirmation}")
                    await ctx.reply(FlightReserved(booking_id, confirmation))

            case CancelFlightReservation(booking_id):
                await asyncio.sleep(0.05)
                conf = reservations.pop(booking_id, None)
                print(f"  [Flight] Cancelled reservation: {conf or 'N/A'}")
                await ctx.reply(FlightCancelled(booking_id))


@actor
async def payment_service(should_fail: bool = False, *, mailbox: Mailbox[PaymentMsg]):
    charges: dict[str, float] = {}

    async for msg, ctx in mailbox:
        match msg:
            case ChargePayment(booking_id, amount):
                await asyncio.sleep(0.1)
                if should_fail:
                    print(f"  [Payment] Charge FAILED for ${amount:.2f}")
                    await ctx.reply(PaymentFailed(booking_id, "Card declined"))
                else:
                    transaction_id = f"TXN-{booking_id[:8]}"
                    charges[booking_id] = amount
                    print(f"  [Payment] Charged ${amount:.2f}: {transaction_id}")
                    await ctx.reply(PaymentCharged(booking_id, transaction_id))

            case RefundPayment(booking_id):
                await asyncio.sleep(0.05)
                amount = charges.pop(booking_id, 0)
                print(f"  [Payment] Refunded ${amount:.2f}")
                await ctx.reply(PaymentRefunded(booking_id))


@actor
async def saga_orchestrator(
    hotel_svc: LocalActorRef[HotelMsg],
    flight_svc: LocalActorRef[FlightMsg],
    payment_svc: LocalActorRef[PaymentMsg],
    *,
    mailbox: Mailbox[BookTrip | SagaResponse],
):
    sagas: dict[str, SagaState] = {}

    async for msg, ctx in mailbox:
        match msg:
            case BookTrip() as booking:
                print(f"[Saga] Starting booking saga: {booking.booking_id}")
                sagas[booking.booking_id] = SagaState(
                    booking=booking,
                    sender=ctx.sender,
                )
                print(f"[Saga] Step 1: Reserving hotel...")
                await hotel_svc.send(
                    ReserveHotel(booking.booking_id, booking.hotel, booking.nights),
                    sender=ctx.self_id,
                )

            case HotelReserved(booking_id, confirmation):
                saga = sagas.get(booking_id)
                if saga:
                    saga.hotel_confirmation = confirmation
                    print(f"[Saga] Step 2: Reserving flight...")
                    await flight_svc.send(
                        ReserveFlight(saga.booking.booking_id, saga.booking.flight),
                        sender=ctx.self_id,
                    )

            case HotelFailed(booking_id, reason):
                saga = sagas.get(booking_id)
                if saga:
                    await _complete_saga(saga, False, f"Hotel booking failed: {reason}", sagas)

            case FlightReserved(booking_id, confirmation):
                saga = sagas.get(booking_id)
                if saga:
                    saga.flight_confirmation = confirmation
                    print(f"[Saga] Step 3: Charging payment...")
                    await payment_svc.send(
                        ChargePayment(saga.booking.booking_id, saga.booking.total_amount),
                        sender=ctx.self_id,
                    )

            case FlightFailed(booking_id, reason):
                saga = sagas.get(booking_id)
                if saga:
                    saga.compensating = True
                    print(f"[Saga] Flight failed, starting compensation...")
                    await hotel_svc.send(
                        CancelHotelReservation(booking_id),
                        sender=ctx.self_id,
                    )
                    saga.step = "compensate_hotel"

            case PaymentCharged(booking_id, transaction_id):
                saga = sagas.get(booking_id)
                if saga:
                    saga.payment_transaction = transaction_id
                    await _complete_saga(saga, True, "Booking completed successfully!", sagas)

            case PaymentFailed(booking_id, reason):
                saga = sagas.get(booking_id)
                if saga:
                    saga.compensating = True
                    print(f"[Saga] Payment failed, starting compensation...")
                    await flight_svc.send(
                        CancelFlightReservation(booking_id),
                        sender=ctx.self_id,
                    )
                    saga.step = "compensate_flight"

            case FlightCancelled(booking_id):
                saga = sagas.get(booking_id)
                if saga and saga.compensating:
                    await hotel_svc.send(
                        CancelHotelReservation(booking_id),
                        sender=ctx.self_id,
                    )
                    saga.step = "compensate_hotel"

            case HotelCancelled(booking_id):
                saga = sagas.get(booking_id)
                if saga and saga.compensating:
                    await _complete_saga(saga, False, "Booking failed - all reservations rolled back", sagas)


async def _complete_saga(saga: SagaState, success: bool, message: str, sagas: dict) -> None:
    print(f"[Saga] Completed: {'SUCCESS' if success else 'FAILED'} - {message}")
    result = BookingResult(
        booking_id=saga.booking.booking_id,
        success=success,
        message=message,
        hotel_confirmation=saga.hotel_confirmation if success else None,
        flight_confirmation=saga.flight_confirmation if success else None,
        payment_transaction=saga.payment_transaction if success else None,
    )
    if saga.sender:
        await saga.sender.send(result)
    del sagas[saga.booking.booking_id]


async def main():
    print("=" * 60)
    print("Saga Pattern Example (Distributed Transactions)")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        print("Scenario 1: Successful booking")
        print("-" * 40)

        hotel1 = await system.actor(hotel_service(should_fail=False), name="hotel-service-1")
        flight1 = await system.actor(flight_service(should_fail=False), name="flight-service-1")
        payment1 = await system.actor(payment_service(should_fail=False), name="payment-service-1")

        saga1 = await system.actor(
            saga_orchestrator(hotel_svc=hotel1, flight_svc=flight1, payment_svc=payment1),
            name="saga-orchestrator-1",
        )

        result1: BookingResult = await saga1.ask(
            BookTrip(
                booking_id="BOOK-001",
                hotel="Grand Hotel",
                nights=3,
                flight="AA-123",
                total_amount=1500.00,
            ),
            timeout=5.0,
        )

        print()
        print(f"Result: {result1.message}")
        if result1.success:
            print(f"  Hotel: {result1.hotel_confirmation}")
            print(f"  Flight: {result1.flight_confirmation}")
            print(f"  Payment: {result1.payment_transaction}")

        print()
        print()
        print("Scenario 2: Payment fails -> rollback hotel and flight")
        print("-" * 40)

        hotel2 = await system.actor(hotel_service(should_fail=False), name="hotel-service-2")
        flight2 = await system.actor(flight_service(should_fail=False), name="flight-service-2")
        payment2 = await system.actor(payment_service(should_fail=True), name="payment-service-2")

        saga2 = await system.actor(
            saga_orchestrator(hotel_svc=hotel2, flight_svc=flight2, payment_svc=payment2),
            name="saga-orchestrator-2",
        )

        result2: BookingResult = await saga2.ask(
            BookTrip(
                booking_id="BOOK-002",
                hotel="Beach Resort",
                nights=5,
                flight="UA-456",
                total_amount=2500.00,
            ),
            timeout=5.0,
        )

        print()
        print(f"Result: {result2.message}")

        print()
        print()
        print("Scenario 3: Flight fails -> rollback hotel only")
        print("-" * 40)

        hotel3 = await system.actor(hotel_service(should_fail=False), name="hotel-service-3")
        flight3 = await system.actor(flight_service(should_fail=True), name="flight-service-3")
        payment3 = await system.actor(payment_service(should_fail=False), name="payment-service-3")

        saga3 = await system.actor(
            saga_orchestrator(hotel_svc=hotel3, flight_svc=flight3, payment_svc=payment3),
            name="saga-orchestrator-3",
        )

        result3: BookingResult = await saga3.ask(
            BookTrip(
                booking_id="BOOK-003",
                hotel="Mountain Lodge",
                nights=2,
                flight="DL-789",
                total_amount=800.00,
            ),
            timeout=5.0,
        )

        print()
        print(f"Result: {result3.message}")

        print()
        print("=" * 60)
        print("Summary:")
        print("  Scenario 1: All steps succeeded")
        print("  Scenario 2: Payment failed -> rolled back flight and hotel")
        print("  Scenario 3: Flight failed -> rolled back hotel only")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
