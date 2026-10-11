"""Retained buffer capacity must not replace the delivered bar counter."""

import math
from datetime import datetime, timedelta, timezone
from types import MethodType

import pytest

import backtrader as bt


class PollClock(datetime):
    """A replay poll occurs at its observed bar time, not today's wall time."""

    current = datetime(2026, 1, 1, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        return (
            cls.current.replace(tzinfo=None)
            if tz is None
            else cls.current.astimezone(tz)
        )


class MinuteSource(bt.feed.DataBase):
    params = (("bars", 10031), ("empty_polls", False))

    def start(self):
        super().start()
        self._number = 0
        self._empty_at = set()
        PollClock.current = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def _load(self):
        if self._number == self.p.bars:
            return False
        if (
            self.p.empty_polls
            and self._number
            and self._number % 997 == 0
            and self._number not in self._empty_at
        ):
            self._empty_at.add(self._number)
            self.put_notification(self.DISCONNECTED)
            self.put_notification(self.CONNECTED)
            return None
        number = self._number
        self._number += 1
        PollClock.current = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(
            minutes=number
        )
        self.datetime[0] = bt.date2num(datetime(2026, 1, 1) + timedelta(minutes=number))
        price = 100 + (number % 113) / 10
        for name, value in (
            ("open", price),
            ("high", price + 2),
            ("low", price - 1),
            ("close", price + 0.5),
            ("volume", number % 7 + 1),
            ("openinterest", 0),
        ):
            getattr(self.lines, name)[0] = value
        return True


def finite(value):
    return value if math.isfinite(value) else None


class ObserveBounded(bt.Strategy):
    params = (("retained", 0),)

    def __init__(self):
        self.fast_mean = bt.ind.SMA(self.data0.close, period=17)
        self.slow_mean = bt.ind.SMA(self.data1.close, period=5)
        self.events = []
        self.aggregates = []
        self.notifications = []
        self.data_notifications = []
        self.orders = []
        self.capacities = [0, 0]
        self._last_aggregate = 0
        if self.p.retained:
            original = self.qbuffer

            def configure(instance, savemem=0, replaying=False):
                original(savemem=savemem, replaying=replaying)
                for data in instance.datas:
                    for line in data.lines:
                        line.minbuffer(max(line._minperiod, instance.p.retained))

            self.qbuffer = MethodType(configure, self)

    def next(self):
        self.events.append(
            (
                self.datetime[0],
                tuple(
                    (len(data), data.datetime[0] if len(data) else None)
                    for data in self.datas
                ),
                finite(self.fast_mean[0]),
                finite(self.slow_mean[0]),
            )
        )
        for index, data in enumerate(self.datas):
            self.capacities[index] = max(self.capacities[index], data.buflen())
        if len(self.data1) == self._last_aggregate:
            return
        self._last_aggregate = len(self.data1)
        self.aggregates.append(
            tuple(
                getattr(self.data1.lines, name)[0]
                for name in ("datetime", "open", "high", "low", "close", "volume")
            )
        )
        # A sparse, deterministic real broker cycle avoids order reference
        # equality while checking order timing, commissions and holdings.
        if self._last_aggregate % 7 == 0:
            order = (
                self.close(data=self.data0)
                if self.position
                else self.buy(data=self.data0, size=3)
            )
            if order is not None:
                self.orders.append(order)

    prenext = next

    def notify_order(self, order):
        ordinal = next(
            index
            for index, original in enumerate(self.orders)
            if original.ref == order.ref
        )
        self.notifications.append(
            (
                ordinal,
                order.status,
                order.executed.dt,
                order.executed.size,
                order.executed.price,
                order.executed.comm,
            )
        )

    def notify_data(self, data, status, *args, **kwargs):
        self.data_notifications.append((self.datas.index(data), status))


def run_minutes(clone, exactbars, retained=0, empty_polls=False, bars=10031):
    cerebro = bt.Cerebro(stdstats=False, exactbars=exactbars)
    cerebro.broker.setcommission(commission=0.0005)
    source = cerebro.adddata(
        MinuteSource(timeframe=bt.TimeFrame.Minutes, empty_polls=empty_polls, bars=bars)
    )
    cerebro.resampledata(
        (
            source
            if clone
            else MinuteSource(
                timeframe=bt.TimeFrame.Minutes, empty_polls=empty_polls, bars=bars
            )
        ),
        timeframe=bt.TimeFrame.Minutes,
        compression=15,
    )
    cerebro.addstrategy(ObserveBounded, retained=retained)
    (strategy,) = cerebro.run(runonce=False, preload=False)
    return strategy, cerebro


def expected_minute_aggregates(bars=10031):
    """Independently aggregate the generated source on 15-minute right edges."""
    groups = [[0]]
    groups.extend(range(start, min(start + 15, bars)) for start in range(1, bars, 15))
    results = []
    for group in groups:
        prices = [100 + (number % 113) / 10 for number in group]
        edge = ((group[-1] + 14) // 15) * 15
        results.append(
            (
                bt.date2num(datetime(2026, 1, 1) + timedelta(minutes=edge)),
                prices[0],
                max(prices) + 2,
                min(prices) - 1,
                prices[-1] + 0.5,
                sum(number % 7 + 1 for number in group),
            )
        )
    return results


@pytest.mark.parametrize("clone", [False, True])
@pytest.mark.parametrize("retained", [0, 2000])
@pytest.mark.parametrize("empty_polls", [False, True])
def test_long_minute_replay_preserves_bars_indicators_and_financial_orders(
    clone, retained, empty_polls, monkeypatch
):
    if empty_polls:
        import backtrader.resamplerfilter as resamplerfilter

        monkeypatch.setattr(resamplerfilter, "datetime", PollClock)
    bars = 30031 if retained else 10031
    full, full_engine = run_minutes(clone, False, empty_polls=empty_polls, bars=bars)
    bounded, bounded_engine = run_minutes(clone, 1, retained, empty_polls, bars)
    assert bounded.events == full.events
    assert bounded.aggregates == full.aggregates
    assert bounded.aggregates == expected_minute_aggregates(bars)
    assert len(bounded.aggregates) > 600
    assert bounded.events[-1][1][0][0] == bars
    assert bounded.notifications == full.notifications
    assert bounded.data_notifications == full.data_notifications
    assert bounded_engine.broker.getcash() == full_engine.broker.getcash()
    assert bounded_engine.broker.getvalue() == full_engine.broker.getvalue()
    assert bounded.position.size == full.position.size
    assert any(item[1] == bt.Order.Completed for item in bounded.notifications)
    assert bounded.capacities[0] <= max(17, retained) + 1
    assert bounded.capacities[1] <= max(5, retained) + 1
    if retained:
        # This exercises repeated eviction past the platform's 2,000 bar
        # baseline, rather than merely configuring a deque that never fills.
        assert bounded.capacities[0] >= retained
        assert bounded.capacities[1] >= retained
    assert not bounded_engine._dopreload


@pytest.mark.parametrize("clone", [False, True])
@pytest.mark.parametrize("runonce", [False, True])
@pytest.mark.parametrize("slow", [False, True])
def test_daily_rewind_hides_future_aggregate_after_capacity_is_reached(
    clone, runonce, slow
):
    from test_data_clone_resampling import expected_aggregates, run_case

    days = tuple(range(1, 29))
    full, _ = run_case(clone, days, runonce=runonce, preload=False, slow=slow)
    bounded, _ = run_case(
        clone, days, runonce=runonce, preload=False, exactbars=1, slow=slow
    )
    assert len(full.events) == len(bounded.events) == 29
    assert bounded.events == full.events
    assert bounded.aggregates == full.aggregates == expected_aggregates(days, 2)
    assert bounded.aggregates[-1][1:] == (127, 138, 117, 133, 55)
    assert bounded.notifications == full.notifications
    assert bounded.maximum_buffers[0] <= 4
    assert bounded.maximum_buffers[-1] <= 3


@pytest.mark.parametrize("clone", [False, True])
def test_bounded_replay_and_resampled_feeds_preserve_shared_clock(clone):
    from test_data_clone_resampling import DailySource, ObserveClone

    def run(exactbars):
        engine = bt.Cerebro(stdstats=False, exactbars=exactbars)
        primary = engine.adddata(DailySource(days=tuple(range(1, 29))))
        engine.replaydata(
            primary if clone else DailySource(days=tuple(range(1, 29))),
            timeframe=bt.TimeFrame.Days,
            compression=2,
        )
        engine.resampledata(
            primary if clone else DailySource(days=tuple(range(1, 29))),
            timeframe=bt.TimeFrame.Days,
            compression=3,
        )
        engine.addstrategy(ObserveClone)
        (strategy,) = engine.run(runonce=False, preload=False)
        return strategy

    full, bounded = run(False), run(1)
    assert bounded.events == full.events
    assert bounded.aggregates == full.aggregates
    assert bounded.source_bars == full.source_bars
    assert bounded.notifications == full.notifications
