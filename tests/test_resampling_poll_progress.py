"""Empty polls cannot authorize resampled read-ahead as source exhaustion."""

import math
from datetime import datetime, timedelta, timezone

import pytest

import backtrader as bt
import backtrader.resamplerfilter as resamplerfilter


class MinutePollFeed(bt.feed.DataBase):
    params = (
        ("minutes", tuple(range(47))),
        ("polls", ()),
        ("live", False),
    )

    def start(self):
        super().start()
        self.remaining = iter(self.p.minutes)
        self.pending = None
        self.polled = set()

    def _load(self):
        if self.pending is None:
            self.pending = next(self.remaining, None)
        if self.pending is None:
            return False
        minute = self.pending
        if minute in self.p.polls and minute not in self.polled:
            self.polled.add(minute)
            self.put_notification(self.DISCONNECTED)
            self.put_notification(self.CONNECTED)
            if self.p.live:
                self.put_notification(self.LIVE)
            return None
        self.pending = None
        self.datetime[0] = bt.date2num(datetime(2026, 1, 1) + timedelta(minutes=minute))
        for name, value in (
            ("open", 100 + minute),
            ("high", 102 + minute),
            ("low", 99 + minute),
            ("close", 101 + minute),
            ("volume", minute + 1),
            ("openinterest", 0),
        ):
            getattr(self.lines, name)[0] = value
        return True


def finite(value):
    return value if math.isfinite(value) else None


class ObservePollProgress(bt.Strategy):
    """Trade every newly visible aggregate without timestamp strategy guards."""

    def __init__(self):
        self.average = bt.ind.SMA(self.datas[-1].close, period=2)
        self.events = []
        self.aggregates = []
        self.decisions = []
        self.orders = []
        self.notifications = []
        self.statuses = []
        self.last = 0

    def next(self):
        aggregate = self.datas[-1]
        self.events.append(
            (
                self.datetime[0],
                tuple(
                    (len(data), data.datetime[0] if len(data) else None)
                    for data in self.datas
                ),
                finite(self.average[0]) if len(self.average) else None,
            )
        )
        if len(aggregate) == self.last:
            return
        self.last = len(aggregate)
        self.aggregates.append(
            tuple(
                getattr(aggregate.lines, name)[0]
                for name in ("datetime", "open", "high", "low", "close", "volume")
            )
        )
        self.decisions.append((self.data0.datetime[0], aggregate.datetime[0]))
        self.orders.append(self.buy(data=self.data0, size=1))

    prenext = next

    def notify_order(self, order):
        ordinal = next(
            i for i, original in enumerate(self.orders) if original.ref == order.ref
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
        self.statuses.append((self.datas.index(data), status))


def run_poll_progress(
    clone,
    exactbars,
    polls=(),
    extra="none",
    live=False,
    minutes=tuple(range(47)),
    replay=False,
):
    engine = bt.Cerebro(stdstats=False, exactbars=exactbars)
    engine.broker.setcommission(commission=0.0005)
    source = engine.adddata(
        MinutePollFeed(
            timeframe=bt.TimeFrame.Minutes, polls=polls, live=live, minutes=minutes
        )
    )
    if extra == "ended":
        engine.adddata(
            MinutePollFeed(timeframe=bt.TimeFrame.Minutes, minutes=(0, 1, 2))
        )
    elif extra == "slow":
        engine.adddata(
            MinutePollFeed(
                timeframe=bt.TimeFrame.Minutes,
                compression=10,
                minutes=(0, 10, 20, 30, 40),
            )
        )
    elif extra == "waiting":
        engine.adddata(
            MinutePollFeed(timeframe=bt.TimeFrame.Minutes, polls=(7, 22, 37))
        )
    if replay:
        engine.replaydata(
            (
                source
                if clone
                else MinutePollFeed(
                    timeframe=bt.TimeFrame.Minutes,
                    polls=polls,
                    live=live,
                    minutes=minutes,
                )
            ),
            timeframe=bt.TimeFrame.Minutes,
            compression=5,
        )
    producer = (
        source
        if clone
        else MinutePollFeed(
            timeframe=bt.TimeFrame.Minutes, polls=polls, live=live, minutes=minutes
        )
    )
    engine.resampledata(producer, timeframe=bt.TimeFrame.Minutes, compression=15)
    engine.addstrategy(ObservePollProgress)
    (strategy,) = engine.run(preload=False, runonce=False)
    return strategy, engine


@pytest.mark.parametrize("clone", [False, True])
@pytest.mark.parametrize("exactbars", [False, 1])
@pytest.mark.parametrize("extra", ["none", "ended"])
def test_unguarded_polls_preserve_bars_indicator_callbacks_and_real_fills(
    clone, exactbars, extra, monkeypatch
):
    class NoWallClock:
        @staticmethod
        def now(tz):
            raise AssertionError("backfill polls must use the processed source clock")

    monkeypatch.setattr(resamplerfilter, "datetime", NoWallClock)
    baseline, baseline_engine = run_poll_progress(clone, exactbars, extra=extra)
    polled, polled_engine = run_poll_progress(
        clone, exactbars, polls=(7, 22, 37), extra=extra
    )
    assert polled.events == baseline.events
    assert polled.aggregates == baseline.aggregates
    assert polled.decisions == baseline.decisions
    assert polled.notifications == baseline.notifications
    completed = [
        event for event in polled.notifications if event[1] == bt.Order.Completed
    ]
    assert [bt.num2date(event[2]).minute for event in completed] == [1, 16, 31, 46]
    assert len(polled.orders) == len(baseline.orders) == 5
    assert polled.orders[-1].status == bt.Order.Submitted
    assert polled_engine.broker.getcash() == baseline_engine.broker.getcash()
    assert polled_engine.broker.getvalue() == baseline_engine.broker.getvalue()
    assert polled.position.size == baseline.position.size == 4
    assert (0, MinutePollFeed.DISCONNECTED) in polled.statuses
    assert (0, MinutePollFeed.CONNECTED) in polled.statuses
    # Generic resampling retains the final partial bucket at exhaustion.
    # This does not certify a complete 15-minute live aggregate.
    assert bt.num2date(polled.aggregates[-1][0]) == datetime(2026, 1, 1, 1)
    assert polled.aggregates[-1][-1] == 47


@pytest.mark.parametrize("clone", [False, True])
@pytest.mark.parametrize("exactbars", [False, 1])
def test_stale_backfill_polls_preserve_all_source_groups_without_clock_patch(
    clone, exactbars
):
    from test_bounded_resampling import expected_minute_aggregates, run_minutes

    # Actual wall time is many months later than these source observations.
    baseline, baseline_engine = run_minutes(clone, exactbars, bars=10031)
    polled, polled_engine = run_minutes(clone, exactbars, empty_polls=True, bars=10031)
    assert polled.events == baseline.events
    assert polled.aggregates == baseline.aggregates == expected_minute_aggregates()
    assert len(polled.aggregates) == 670
    assert polled.notifications == baseline.notifications
    assert polled_engine.broker.getcash() == baseline_engine.broker.getcash()
    assert polled_engine.broker.getvalue() == baseline_engine.broker.getvalue()


@pytest.mark.parametrize("clone", [False, True])
@pytest.mark.parametrize("exactbars", [False, 1])
def test_explicit_live_clock_can_flush_a_gap_without_repeating_financial_identity(
    clone, exactbars, monkeypatch
):
    calls = []

    class LiveClock:
        @staticmethod
        def now(tz):
            calls.append(tz)
            return datetime(2026, 1, 1, 0, 15, tzinfo=timezone.utc).astimezone(tz)

    monkeypatch.setattr(resamplerfilter, "datetime", LiveClock)
    minutes = tuple(range(7)) + tuple(range(16, 21))
    baseline, baseline_engine = run_poll_progress(clone, exactbars, minutes=minutes)
    polled, polled_engine = run_poll_progress(
        clone, exactbars, polls=(16,), live=True, minutes=minutes
    )
    assert polled.aggregates == baseline.aggregates
    assert polled.decisions == baseline.decisions
    assert polled.notifications == baseline.notifications
    assert polled_engine.broker.getcash() == baseline_engine.broker.getcash()
    assert len(polled.orders) == len(baseline.orders)
    assert (0, MinutePollFeed.LIVE) in polled.statuses
    if clone:
        assert calls


@pytest.mark.parametrize("exactbars", [False, 1])
def test_slow_independent_instrument_progresses_while_clone_source_waits(
    exactbars, monkeypatch
):
    class NoWallClock:
        @staticmethod
        def now(tz):
            raise AssertionError("historical polls cannot consult wall time")

    monkeypatch.setattr(resamplerfilter, "datetime", NoWallClock)
    baseline, _ = run_poll_progress(True, exactbars, extra="slow")
    polled, _ = run_poll_progress(True, exactbars, polls=(7, 22, 37), extra="slow")
    assert polled.aggregates == baseline.aggregates
    assert polled.decisions == baseline.decisions
    assert polled.notifications == baseline.notifications
    # The separate instrument can supply its own later observation. This
    # must not freeze its normal feed, or publish a future clone value.
    assert any(event[1][1][1] > event[1][0][1] for event in polled.events)
    for _, feeds, _ in polled.events[:-1]:
        assert feeds[-1][1] <= feeds[0][1]


@pytest.mark.parametrize("clone", [False, True])
@pytest.mark.parametrize("exactbars", [False, 1])
def test_unrelated_waiting_instrument_does_not_freeze_active_source_aggregation(
    clone, exactbars
):
    baseline, _ = run_poll_progress(clone, exactbars)
    polled, _ = run_poll_progress(clone, exactbars, extra="waiting")
    assert polled.aggregates == baseline.aggregates
    assert polled.decisions == baseline.decisions
    assert polled.notifications == baseline.notifications


@pytest.mark.parametrize("clone", [False, True])
def test_first_empty_source_poll_has_no_datetime_to_force(clone):
    baseline, _ = run_poll_progress(clone, False, extra="slow")
    polled, _ = run_poll_progress(clone, False, polls=(0,), extra="slow")
    assert polled.aggregates == baseline.aggregates
    assert polled.decisions == baseline.decisions
    assert polled.notifications == baseline.notifications


@pytest.mark.parametrize("exactbars", [False, 1])
def test_exhausted_clone_source_can_deliver_while_unrelated_normal_feed_waits(
    exactbars,
):
    from test_data_clone_resampling import DailySource

    class WaitingInstrument(DailySource):
        def start(self):
            super().start()
            self.polls_left = 4
            self.loaded = 0

        def _load(self):
            if self.loaded == 1 and self.polls_left:
                self.polls_left -= 1
                return None
            result = super()._load()
            if result:
                self.loaded += 1
            return result

    engine = bt.Cerebro(stdstats=False, exactbars=exactbars)
    source = engine.adddata(DailySource(days=(1, 2)))
    engine.adddata(WaitingInstrument(days=(1, 3)))
    engine.resampledata(source, timeframe=bt.TimeFrame.Days, compression=2)
    engine.addstrategy(ObservePollProgress)
    (strategy,) = engine.run(preload=False, runonce=False)
    assert len(strategy.aggregates) == 1
    aggregate_clock = strategy.aggregates[0][0]
    assert bt.num2date(aggregate_clock).day == 2
    # The source has actually ended, so its final completed aggregate must
    # be observable before the unrelated instrument's next January 3 bar.
    assert any(
        event[0] == aggregate_clock and event[1][1][0] == 1 for event in strategy.events
    )


@pytest.mark.parametrize("clone", [False, True])
@pytest.mark.parametrize("exactbars", [False, 1])
def test_replayed_feed_keeps_poll_clock_while_resampler_waits(clone, exactbars):
    baseline, _ = run_poll_progress(clone, exactbars, replay=True)
    polled, _ = run_poll_progress(clone, exactbars, polls=(7, 22, 37), replay=True)
    assert polled.events == baseline.events
    assert polled.aggregates == baseline.aggregates
    assert polled.decisions == baseline.decisions
    assert polled.notifications == baseline.notifications
