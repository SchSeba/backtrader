"""Cloned aggregates consume every source bar exactly once."""

from datetime import datetime
from types import MethodType

import pytest

import backtrader as bt


class DailySource(bt.feed.DataBase):
    params = (("days", tuple(range(1, 9))),)

    def start(self):
        super().start()
        self._days = iter(self.p.days)

    def _load(self):
        day = next(self._days, None)
        if day is None:
            return False
        self.lines.datetime[0] = bt.date2num(datetime(2026, 1, day))
        self.lines.open[0] = 100 + day
        self.lines.high[0] = 110 + day
        self.lines.low[0] = 90 + day
        self.lines.close[0] = 105 + day
        self.lines.volume[0] = day
        self.lines.openinterest[0] = 0
        return True


class ObserveClone(bt.Strategy):
    params = (("source_window", 0),)

    def __init__(self):
        self.events = []
        self.aggregates = []
        self.source_bars = []
        self.indicator_values = []
        self.orders = []
        self.notifications = []
        self.maximum_buffers = [0 for _ in self.datas]
        self._counts = [0 for _ in self.datas]
        self.primary_mean = bt.ind.SMA(self.data0.close, period=3)
        self.aggregate_mean = bt.ind.SMA(self.datas[-1].close, period=2)
        if self.p.source_window:
            original = self.qbuffer

            def bounded_source(instance, savemem=0, replaying=False):
                original(savemem=savemem, replaying=replaying)
                for line in instance.data0.lines:
                    line.qbuffer(extrasize=1)
                    line.minbuffer(max(line._minperiod, instance.p.source_window))

            self.qbuffer = MethodType(bounded_source, self)

    def next(self):
        self.events.append(
            (
                self.datetime[0],
                tuple(
                    (len(data), data.datetime[0] if len(data) else None)
                    for data in self.datas
                ),
            )
        )
        self.indicator_values.append(
            (
                self.primary_mean[0] if len(self.primary_mean) else None,
                self.aggregate_mean[0] if len(self.aggregate_mean) else None,
            )
        )
        for index, data in enumerate(self.datas):
            self.maximum_buffers[index] = max(
                self.maximum_buffers[index], data.buflen()
            )
            if len(data) == self._counts[index]:
                continue
            self._counts[index] = len(data)
            values = tuple(
                getattr(data.lines, name)[0]
                for name in ("datetime", "open", "high", "low", "close", "volume")
            )
            if index == 0:
                self.source_bars.append(values)
            if index == len(self.datas) - 1:
                self.aggregates.append(values)
                self.orders.append(self.buy(data=self.data0, size=1))

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
            )
        )


def midnight(day):
    return bt.date2num(datetime(2026, 1, day))


def end_of_day(day):
    return bt.date2num(datetime(2026, 1, day, 23, 59, 59, 999990))


def run_case(
    clone,
    days,
    compression=2,
    runonce=False,
    preload=True,
    exactbars=False,
    fromdate=None,
    todate=None,
    slow=False,
    source_window=0,
):
    cerebro = bt.Cerebro(stdstats=False, exactbars=exactbars)
    parameters = dict(days=days, fromdate=fromdate, todate=todate)
    primary = cerebro.adddata(DailySource(**parameters))
    if slow:
        cerebro.adddata(DailySource(**dict(parameters, days=days[::2])))
    source = primary if clone else DailySource(**parameters)
    cerebro.resampledata(source, timeframe=bt.TimeFrame.Days, compression=compression)
    cerebro.addstrategy(ObserveClone, source_window=source_window)
    (strategy,) = cerebro.run(runonce=runonce, preload=preload)
    return strategy, cerebro


def expected_aggregates(days, compression):
    bars = []
    for offset in range(0, len(days), compression):
        group = days[offset : offset + compression]
        bars.append(
            (
                end_of_day(group[-1]),
                100 + group[0],
                110 + max(group),
                90 + min(group),
                105 + group[-1],
                sum(group),
            )
        )
    return bars


@pytest.mark.parametrize("runonce", [False, True])
@pytest.mark.parametrize("preload", [False, True])
@pytest.mark.parametrize("slow", [False, True])
@pytest.mark.parametrize("last_day,compression", [(3, 2), (4, 2), (8, 2), (8, 3)])
def test_clone_and_separate_source_have_identical_full_delivery(
    runonce, preload, slow, last_day, compression
):
    days = tuple(range(1, last_day + 1))
    cloned, _ = run_case(True, days, compression, runonce, preload, slow=slow)
    separate, _ = run_case(False, days, compression, runonce, preload, slow=slow)
    assert cloned.events == separate.events
    assert (
        cloned.aggregates
        == separate.aggregates
        == expected_aggregates(days, compression)
    )
    assert (
        cloned.source_bars
        == separate.source_bars
        == [
            (midnight(day), 100 + day, 110 + day, 90 + day, 105 + day, day)
            for day in days
        ]
    )
    assert cloned.notifications == separate.notifications
    completed = [item for item in cloned.notifications if item[1] == bt.Order.Completed]
    assert len({item[0] for item in completed}) == len(completed)
    assert len(cloned.orders) == len(expected_aggregates(days, compression))


@pytest.mark.parametrize("runonce", [False, True])
@pytest.mark.parametrize("preload", [False, True])
def test_clone_respects_source_date_limits(runonce, preload):
    days = tuple(range(1, 9))
    args = dict(
        runonce=runonce,
        preload=preload,
        fromdate=datetime(2026, 1, 2),
        todate=datetime(2026, 1, 6),
    )
    cloned, _ = run_case(True, days, **args)
    separate, _ = run_case(False, days, **args)
    assert cloned.events == separate.events
    assert (
        cloned.aggregates
        == separate.aggregates
        == expected_aggregates(tuple(range(2, 7)), 2)
    )
    assert [bar[0] for bar in cloned.source_bars] == list(map(midnight, range(2, 7)))


def test_clone_consumption_is_independent_of_bounded_source_history():
    days = tuple(range(1, 29))
    cloned, cerebro = run_case(True, days, preload=False, source_window=3)
    separate, _ = run_case(False, days, preload=False, source_window=3)
    unbounded, _ = run_case(True, days, preload=False)
    assert cloned.events == separate.events == unbounded.events
    assert cloned.aggregates == separate.aggregates == expected_aggregates(days, 2)
    assert cloned.notifications == separate.notifications == unbounded.notifications
    # Minimum periods remain available after old source bars leave the deque.
    assert (
        cloned.indicator_values[-1] == separate.indicator_values[-1] == (132.0, 132.0)
    )
    assert not cerebro._dopreload
    assert cloned.maximum_buffers[0] <= 4


@pytest.mark.parametrize("runonce", [False, True])
@pytest.mark.parametrize("preload", [False, True])
def test_plain_clone_retains_each_primary_bar(runonce, preload):
    cerebro = bt.Cerebro(stdstats=False)
    source = cerebro.adddata(DailySource(days=(1, 2, 3, 4)))
    cerebro.adddata(source.clone())
    cerebro.addstrategy(ObserveClone)
    (strategy,) = cerebro.run(runonce=runonce, preload=preload)
    assert strategy.source_bars == strategy.aggregates
    assert [event[0] for event in strategy.events] == list(map(midnight, range(1, 5)))
