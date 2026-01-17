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
from dataclasses import dataclass, field
from enum import Enum

from casty import Actor, ActorSystem, Context, LocalRef


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


class HotelService(Actor[ReserveHotel | CancelHotelReservation]):
    """Simulates a hotel reservation service."""

    def __init__(self, should_fail: bool = False):
        self.should_fail = should_fail
        self.reservations: dict[str, str] = {}

    async def receive(
        self,
        msg: ReserveHotel | CancelHotelReservation,
        ctx: Context,
    ) -> None:
        match msg:
            case ReserveHotel(booking_id, hotel, nights):
                await asyncio.sleep(0.1)

                if self.should_fail:
                    print(f"  [Hotel] Reservation FAILED for {hotel}")
                    await ctx.reply(HotelFailed(booking_id, "No rooms available"))
                else:
                    confirmation = f"HOTEL-{booking_id[:8]}"
                    self.reservations[booking_id] = confirmation
                    print(f"  [Hotel] Reserved {hotel} for {nights} nights: {confirmation}")
                    await ctx.reply(HotelReserved(booking_id, confirmation))

            case CancelHotelReservation(booking_id):
                await asyncio.sleep(0.05)
                conf = self.reservations.pop(booking_id, None)
                print(f"  [Hotel] Cancelled reservation: {conf or 'N/A'}")
                await ctx.reply(HotelCancelled(booking_id))


class FlightService(Actor[ReserveFlight | CancelFlightReservation]):
    """Simulates a flight reservation service."""

    def __init__(self, should_fail: bool = False):
        self.should_fail = should_fail
        self.reservations: dict[str, str] = {}

    async def receive(
        self,
        msg: ReserveFlight | CancelFlightReservation,
        ctx: Context,
    ) -> None:
        match msg:
            case ReserveFlight(booking_id, flight):
                await asyncio.sleep(0.1)

                if self.should_fail:
                    print(f"  [Flight] Reservation FAILED for {flight}")
                    await ctx.reply(FlightFailed(booking_id, "Flight fully booked"))
                else:
                    confirmation = f"FLIGHT-{booking_id[:8]}"
                    self.reservations[booking_id] = confirmation
                    print(f"  [Flight] Reserved {flight}: {confirmation}")
                    await ctx.reply(FlightReserved(booking_id, confirmation))

            case CancelFlightReservation(booking_id):
                await asyncio.sleep(0.05)
                conf = self.reservations.pop(booking_id, None)
                print(f"  [Flight] Cancelled reservation: {conf or 'N/A'}")
                await ctx.reply(FlightCancelled(booking_id))


class PaymentService(Actor[ChargePayment | RefundPayment]):
    """Simulates a payment service."""

    def __init__(self, should_fail: bool = False):
        self.should_fail = should_fail
        self.charges: dict[str, float] = {}

    async def receive(
        self,
        msg: ChargePayment | RefundPayment,
        ctx: Context,
    ) -> None:
        match msg:
            case ChargePayment(booking_id, amount):
                await asyncio.sleep(0.1)

                if self.should_fail:
                    print(f"  [Payment] Charge FAILED for ${amount:.2f}")
                    await ctx.reply(PaymentFailed(booking_id, "Card declined"))
                else:
                    transaction_id = f"TXN-{booking_id[:8]}"
                    self.charges[booking_id] = amount
                    print(f"  [Payment] Charged ${amount:.2f}: {transaction_id}")
                    await ctx.reply(PaymentCharged(booking_id, transaction_id))

            case RefundPayment(booking_id):
                await asyncio.sleep(0.05)
                amount = self.charges.pop(booking_id, 0)
                print(f"  [Payment] Refunded ${amount:.2f}")
                await ctx.reply(PaymentRefunded(booking_id))


@dataclass
class BookTrip:
    """Request to book a complete trip."""
    booking_id: str
    hotel: str
    nights: int
    flight: str
    total_amount: float


@dataclass
class BookingResult:
    """Result of a booking attempt."""
    booking_id: str
    success: bool
    message: str
    hotel_confirmation: str | None = None
    flight_confirmation: str | None = None
    payment_transaction: str | None = None


SagaResponse = (
    HotelReserved | HotelFailed | HotelCancelled |
    FlightReserved | FlightFailed | FlightCancelled |
    PaymentCharged | PaymentFailed | PaymentRefunded
)


@dataclass
class SagaState:
    """Tracks the state of a saga execution."""
    booking: BookTrip
    sender: LocalRef | None
    hotel_confirmation: str | None = None
    flight_confirmation: str | None = None
    payment_transaction: str | None = None
    step: str = "hotel"
    compensating: bool = False


class SagaOrchestrator(Actor[BookTrip | SagaResponse]):
    """Orchestrates the booking saga with compensation on failure."""

    def __init__(
        self,
        hotel_service: LocalRef,
        flight_service: LocalRef,
        payment_service: LocalRef,
    ):
        self.hotel_service = hotel_service
        self.flight_service = flight_service
        self.payment_service = payment_service
        self.sagas: dict[str, SagaState] = {}

    async def receive(self, msg: BookTrip | SagaResponse, ctx: Context) -> None:
        match msg:
            case BookTrip() as booking:
                print(f"[Saga] Starting booking saga: {booking.booking_id}")
                self.sagas[booking.booking_id] = SagaState(
                    booking=booking,
                    sender=ctx.sender,
                )
                await self._step_reserve_hotel(booking)

            case HotelReserved(booking_id, confirmation):
                saga = self.sagas.get(booking_id)
                if saga:
                    saga.hotel_confirmation = confirmation
                    await self._step_reserve_flight(saga)

            case HotelFailed(booking_id, reason):
                saga = self.sagas.get(booking_id)
                if saga:
                    await self._complete_saga(saga, False, f"Hotel booking failed: {reason}")

            case FlightReserved(booking_id, confirmation):
                saga = self.sagas.get(booking_id)
                if saga:
                    saga.flight_confirmation = confirmation
                    await self._step_charge_payment(saga)

            case FlightFailed(booking_id, reason):
                saga = self.sagas.get(booking_id)
                if saga:
                    saga.compensating = True
                    print(f"[Saga] Flight failed, starting compensation...")
                    await self.hotel_service.send(
                        CancelHotelReservation(booking_id),
                        sender=self._ctx.self_ref,
                    )
                    saga.step = "compensate_hotel"

            case PaymentCharged(booking_id, transaction_id):
                saga = self.sagas.get(booking_id)
                if saga:
                    saga.payment_transaction = transaction_id
                    await self._complete_saga(saga, True, "Booking completed successfully!")

            case PaymentFailed(booking_id, reason):
                saga = self.sagas.get(booking_id)
                if saga:
                    saga.compensating = True
                    print(f"[Saga] Payment failed, starting compensation...")
                    await self.flight_service.send(
                        CancelFlightReservation(booking_id),
                        sender=self._ctx.self_ref,
                    )
                    saga.step = "compensate_flight"

            case FlightCancelled(booking_id):
                saga = self.sagas.get(booking_id)
                if saga and saga.compensating:
                    await self.hotel_service.send(
                        CancelHotelReservation(booking_id),
                        sender=self._ctx.self_ref,
                    )
                    saga.step = "compensate_hotel"

            case HotelCancelled(booking_id):
                saga = self.sagas.get(booking_id)
                if saga and saga.compensating:
                    await self._complete_saga(saga, False, "Booking failed - all reservations rolled back")

    async def _step_reserve_hotel(self, booking: BookTrip) -> None:
        print(f"[Saga] Step 1: Reserving hotel...")
        await self.hotel_service.send(
            ReserveHotel(booking.booking_id, booking.hotel, booking.nights),
            sender=self._ctx.self_ref,
        )

    async def _step_reserve_flight(self, saga: SagaState) -> None:
        print(f"[Saga] Step 2: Reserving flight...")
        await self.flight_service.send(
            ReserveFlight(saga.booking.booking_id, saga.booking.flight),
            sender=self._ctx.self_ref,
        )

    async def _step_charge_payment(self, saga: SagaState) -> None:
        print(f"[Saga] Step 3: Charging payment...")
        await self.payment_service.send(
            ChargePayment(saga.booking.booking_id, saga.booking.total_amount),
            sender=self._ctx.self_ref,
        )

    async def _complete_saga(self, saga: SagaState, success: bool, message: str) -> None:
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
        del self.sagas[saga.booking.booking_id]


async def main():
    print("=" * 60)
    print("Saga Pattern Example (Distributed Transactions)")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        print("Scenario 1: Successful booking")
        print("-" * 40)

        hotel1 = await system.spawn(HotelService, should_fail=False)
        flight1 = await system.spawn(FlightService, should_fail=False)
        payment1 = await system.spawn(PaymentService, should_fail=False)

        saga1 = await system.spawn(
            SagaOrchestrator,
            hotel_service=hotel1,
            flight_service=flight1,
            payment_service=payment1,
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

        hotel2 = await system.spawn(HotelService, should_fail=False)
        flight2 = await system.spawn(FlightService, should_fail=False)
        payment2 = await system.spawn(PaymentService, should_fail=True)

        saga2 = await system.spawn(
            SagaOrchestrator,
            hotel_service=hotel2,
            flight_service=flight2,
            payment_service=payment2,
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

        hotel3 = await system.spawn(HotelService, should_fail=False)
        flight3 = await system.spawn(FlightService, should_fail=True)
        payment3 = await system.spawn(PaymentService, should_fail=False)

        saga3 = await system.spawn(
            SagaOrchestrator,
            hotel_service=hotel3,
            flight_service=flight3,
            payment_service=payment3,
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
