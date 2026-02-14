"""Pipe to Self — integrate with external APIs without blocking the mailbox."""

import asyncio
from dataclasses import dataclass

from casty import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors


# ── Simulated external API ───────────────────────────────────────────


async def fetch_weather(city: str) -> dict[str, str | float]:
    """Simulate a slow network call to a weather API."""
    await asyncio.sleep(0.5)
    forecasts: dict[str, dict[str, str | float]] = {
        "london": {"city": "London", "temp": 12.5, "sky": "cloudy"},
        "tokyo": {"city": "Tokyo", "temp": 28.0, "sky": "sunny"},
        "paris": {"city": "Paris", "temp": 18.3, "sky": "rainy"},
    }
    if city.lower() not in forecasts:
        raise ValueError(f"Unknown city: {city}")
    return forecasts[city.lower()]


# ── Messages ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class FetchWeather:
    city: str


@dataclass(frozen=True)
class WeatherResult:
    city: str
    temp: float
    sky: str


@dataclass(frozen=True)
class WeatherFailed:
    city: str
    error: str


@dataclass(frozen=True)
class Ping:
    reply_to: ActorRef[str]


@dataclass(frozen=True)
class GetForecasts:
    reply_to: ActorRef[tuple[WeatherResult, ...]]


type WeatherMsg = FetchWeather | WeatherResult | WeatherFailed | Ping | GetForecasts


# ── Behavior ─────────────────────────────────────────────────────────


def weather_station(
    forecasts: tuple[WeatherResult, ...] = (),
) -> Behavior[WeatherMsg]:
    async def receive(
        ctx: ActorContext[WeatherMsg], msg: WeatherMsg
    ) -> Behavior[WeatherMsg]:
        match msg:
            case FetchWeather(city):
                ctx.pipe_to_self(
                    fetch_weather(city),
                    lambda data: WeatherResult(
                        city=str(data["city"]),
                        temp=float(data["temp"]),
                        sky=str(data["sky"]),
                    ),
                    on_failure=lambda exc: WeatherFailed(
                        city=city, error=str(exc)
                    ),
                )
                return Behaviors.same()

            case WeatherResult() as result:
                print(f"  Weather in {result.city}: {result.temp}C, {result.sky}")
                return weather_station((*forecasts, result))

            case WeatherFailed(city, error):
                print(f"  Failed to fetch {city}: {error}")
                return Behaviors.same()

            case Ping(reply_to):
                reply_to.tell("pong")
                return Behaviors.same()

            case GetForecasts(reply_to):
                reply_to.tell(forecasts)
                return Behaviors.same()

    return Behaviors.receive(receive)


# ── Main ─────────────────────────────────────────────────────────────


async def main() -> None:
    async with ActorSystem() as system:
        station: ActorRef[WeatherMsg] = system.spawn(
            weather_station(), "weather-station"
        )

        # Fire off three fetch requests — all run concurrently in the background
        print("── Requesting forecasts (non-blocking) ──")
        station.tell(FetchWeather("london"))
        station.tell(FetchWeather("tokyo"))
        station.tell(FetchWeather("narnia"))  # will fail

        # The mailbox is NOT blocked — Ping is processed immediately
        reply = await system.ask(
            station, lambda r: Ping(reply_to=r), timeout=1.0
        )
        print(f"  Ping reply: {reply} (mailbox is responsive!)")

        # Wait for all fetches to complete
        await asyncio.sleep(1.0)

        # Read the collected results
        results = await system.ask(
            station, lambda r: GetForecasts(reply_to=r), timeout=1.0
        )
        print(f"\n── Collected {len(results)} forecasts ──")
        for r in results:
            print(f"  {r.city}: {r.temp}C, {r.sky}")


asyncio.run(main())
