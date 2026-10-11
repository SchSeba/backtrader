"""UTC empty-poll checks preserve feed time and completed-bar identities."""

from datetime import datetime, time, timedelta, timezone
from types import SimpleNamespace

import pytest

import backtrader as bt
from backtrader import resamplerfilter
from backtrader.utils.date import UTC


@pytest.mark.parametrize("offset", [timedelta(), timedelta(minutes=7, seconds=3)])
@pytest.mark.parametrize("zone", [timezone.utc, timezone(timedelta(hours=-4))])
def test_empty_poll_clock_is_utc_with_offset_and_localized_output(
    monkeypatch, offset, zone
):
    instant = datetime(2026, 10, 11, 14, 32, 11, tzinfo=timezone.utc)
    calls = []

    class Clock:
        @staticmethod
        def now(tz):
            calls.append(tz)
            return instant.astimezone(tz)

    monkeypatch.setattr(resamplerfilter, "datetime", Clock)
    data = SimpleNamespace(
        _timeoffset=lambda: offset,
        num2date=lambda value: bt.num2date(value, tz=zone),
        p=SimpleNamespace(sessionend=time(20)),
    )
    fake = resamplerfilter.DTFaker(data)
    expected = instant + offset
    assert calls == [UTC]
    assert fake[0] == bt.date2num(expected)
    assert fake.datetime() == expected.astimezone(zone).replace(tzinfo=None)
    assert fake.date() == expected.astimezone(zone).date()
    assert fake.time() == expected.astimezone(zone).time().replace(tzinfo=None)
    assert fake.sessionend == time(20)
    assert fake[-1] == float("-inf")


def test_forced_feed_clock_does_not_read_wall_clock_or_offset(monkeypatch):
    def unexpected(*args):
        raise AssertionError("forced feed must retain its own clock")

    monkeypatch.setattr(resamplerfilter, "datetime", SimpleNamespace(now=unexpected))
    local = datetime(2026, 10, 11, 10, 32, 11)
    numeric = bt.date2num(datetime(2026, 10, 11, 14, 32, 11))

    class ForcedClock:
        def __getitem__(self, index):
            assert index == 0
            return numeric

        def datetime(self):
            return local

    data = SimpleNamespace(
        _timeoffset=unexpected, p=SimpleNamespace(sessionend=time(20))
    )
    fake = resamplerfilter.DTFaker(data, SimpleNamespace(datetime=ForcedClock()))
    assert fake[0] == numeric
    assert fake.datetime() == local


class PollSource(bt.feed.DataBase):
    params = (("empty_polls", False),)

    def start(self):
        super().start()
        self.number = 0
        self.polled = set()

    def _load(self):
        if self.number == 47:
            return False
        if self.p.empty_polls and self.number in (7, 22, 37):
            if self.number not in self.polled:
                self.polled.add(self.number)
                self.put_notification(self.DISCONNECTED)
                self.put_notification(self.CONNECTED)
                return None
        number = self.number
        self.number += 1
        self.datetime[0] = bt.date2num(datetime(2026, 1, 1) + timedelta(minutes=number))
        for name, value in (
            ("open", 100 + number),
            ("high", 102 + number),
            ("low", 99 + number),
            ("close", 101 + number),
            ("volume", number + 1),
            ("openinterest", 0),
        ):
            getattr(self.lines, name)[0] = value
        return True


class PollObserve(bt.Strategy):
    def __init__(self):
        self.aggregates = []
        self.orders = []
        self.executions = []
        self.statuses = []
        self.last = 0
        self.observed = 0

    def next(self):
        if len(self.data1) != self.observed:
            self.observed = len(self.data1)
            self.aggregates.append(
                tuple(
                    getattr(self.data1.lines, name)[0]
                    for name in ("datetime", "open", "high", "low", "close", "volume")
                )
            )
        # Independent feeds may poll at different moments. A decision using
        # both feeds requires the aggregate to be completed on the primary
        # clock, just as the live strategy contract requires for every input.
        if self.data1.datetime[0] > self.data0.datetime[0]:
            return
        if len(self.data1) == self.last:
            return
        self.last = len(self.data1)
        self.orders.append(self.buy(data=self.data0, size=1))

    prenext = next

    def notify_order(self, order):
        if order.status == order.Completed:
            self.executions.append(
                (order.executed.dt, order.executed.size, order.executed.price)
            )

    def notify_data(self, data, status, *args, **kwargs):
        self.statuses.append((self.datas.index(data), status))


def run_poll_engine(clone, empty_polls):
    engine = bt.Cerebro(stdstats=False)
    source = engine.adddata(
        PollSource(empty_polls=empty_polls, timeframe=bt.TimeFrame.Minutes)
    )
    aggregate = (
        source
        if clone
        else PollSource(empty_polls=empty_polls, timeframe=bt.TimeFrame.Minutes)
    )
    engine.resampledata(aggregate, timeframe=bt.TimeFrame.Minutes, compression=15)
    engine.addstrategy(PollObserve)
    (strategy,) = engine.run(preload=False, runonce=False)
    return strategy, engine


@pytest.mark.parametrize("clone", [False, True])
def test_empty_polls_and_reconnect_preserve_completed_bars_and_orders(
    monkeypatch, clone
):
    clock_calls = []

    class Clock:
        @staticmethod
        def now(tz):
            clock_calls.append(tz)
            # Before the fixture's next aggregation boundary: an empty poll
            # cannot manufacture a new completed bar in this scenario.
            return datetime(2026, 1, 1, tzinfo=timezone.utc).astimezone(tz)

    monkeypatch.setattr(resamplerfilter, "datetime", Clock)
    baseline, baseline_engine = run_poll_engine(clone, False)
    polled, polled_engine = run_poll_engine(clone, True)
    if clone:
        assert clock_calls and all(tz is UTC for tz in clock_calls)
    else:
        # A separate resampler synchronizes to the primary feed's forced
        # clock; it must not replace that clock with wall time.
        assert not clock_calls
    assert polled.aggregates == baseline.aggregates
    assert len(polled.aggregates) == 5
    assert [bt.num2date(bar[0]).minute for bar in polled.aggregates] == [
        0,
        15,
        30,
        45,
        0,
    ]
    assert len({bar[0] for bar in polled.aggregates}) == 5
    assert polled.executions == baseline.executions
    assert len(polled.executions) == 4
    assert len(polled.orders) == len(baseline.orders) == 4
    assert polled_engine.broker.getcash() == baseline_engine.broker.getcash()
    assert polled.position.size == baseline.position.size == 4
    assert (0, PollSource.DISCONNECTED) in polled.statuses
    assert (0, PollSource.CONNECTED) in polled.statuses
