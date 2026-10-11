"""End-of-input bars must retain their timestamps and advance the event clock."""

from datetime import datetime, time, timedelta

import pytest

import backtrader as bt


class DailyFeed(bt.feed.DataBase):
    params = (("days", ()),)

    def start(self):
        super().start()
        self._days = iter(self.p.days)

    def _load(self):
        day = next(self._days, None)
        if day is None:
            return False
        stamp = day if isinstance(day, datetime) else datetime(2026, 1, day)
        self.lines.datetime[0] = bt.date2num(stamp)
        price = 100 + len(self)
        for name in ("open", "high", "low", "close"):
            getattr(self.lines, name)[0] = price
        self.lines.volume[0] = 1
        self.lines.openinterest[0] = 0
        return True


class RecordDelivery(bt.Strategy):
    params = (("timer_when", time(23, 59)), ("timer_repeat", timedelta()))

    def __init__(self):
        self.events = []
        self.timers = []
        for cheat in (False, True):
            self.add_timer(when=self.p.timer_when, repeat=self.p.timer_repeat,
                           cheat=cheat)

    def next(self):
        self.events.append((
            self.datetime[0],
            self.cerebro._udtmaster,
            tuple((len(data), data.datetime[0] if len(data) else None)
                  for data in self.datas),
        ))

    prenext = next

    def notify_timer(self, timer, when):
        self.timers.append((timer.p.cheat, when, self.cerebro._udtmaster))


def midnight(day):
    return bt.date2num(datetime(2026, 1, day))


def session_end(day):
    return bt.date2num(datetime(2026, 1, day, 23, 59, 59, 999990))


def run_delivery(days, runonce, preload=True, slow=True, compressions=(2,),
                 todate=None, clone=False):
    cerebro = bt.Cerebro(stdstats=False)
    fast = cerebro.adddata(DailyFeed(days=days, todate=todate))
    if slow:
        cerebro.adddata(DailyFeed(days=days[::2], todate=todate))
    for compression in compressions:
        source = fast if clone else DailyFeed(days=days, todate=todate)
        cerebro.resampledata(source, timeframe=bt.TimeFrame.Days,
                             compression=compression)
    cerebro.addstrategy(RecordDelivery)
    strategy, = cerebro.run(runonce=runonce, preload=preload)
    return strategy, cerebro


@pytest.mark.parametrize("runonce", [False, True])
@pytest.mark.parametrize("preload", [False, True])
@pytest.mark.parametrize("slow", [False, True])
@pytest.mark.parametrize("last_day", [3, 4, 8])
def test_mixed_feeds_deliver_final_aggregate_once(
        runonce, preload, slow, last_day):
    days = tuple(range(1, last_day + 1))
    strategy, _ = run_delivery(days, runonce, preload, slow)
    events = strategy.events
    expected_dates = [midnight(day) for day in days] + [session_end(last_day)]
    assert [event[0] for event in events] == expected_dates
    assert [event[1] for event in events] == list(map(bt.num2date, expected_dates))
    for day, event in zip(days, events):
        feeds = [(day, midnight(day))]
        if slow:
            slow_day = day if day % 2 else day - 1
            feeds.append(((day + 1) // 2, midnight(slow_day)))
        aggregated = (day - 1) // 2
        feeds.append((aggregated, session_end(aggregated * 2)
                      if aggregated else None))
        assert event[2] == tuple(feeds)
    final_feeds = list(events[-2][2])
    final_feeds[-1] = ((last_day + 1) // 2, session_end(last_day))
    assert events[-1][2] == tuple(final_feeds)
    assert strategy.timers == [
        (cheat, datetime(2026, 1, last_day, 23, 59),
         bt.num2date(session_end(last_day))) for cheat in (True, False)
    ]


@pytest.mark.parametrize("runonce", [False, True])
def test_cloned_feed_final_aggregate_advances_timer_clock(runonce):
    # Four source bars avoid the independently tracked clone source-count bug.
    strategy, _ = run_delivery((1, 2, 3, 4), runonce, clone=True)
    assert [event[0] for event in strategy.events] == [
        midnight(1), midnight(2), midnight(3), midnight(4), session_end(4)]
    assert strategy.events[-1][2] == (
        (4, midnight(4)), (2, midnight(3)), (2, session_end(4)))
    assert strategy.events[-1][1] == bt.num2date(session_end(4))
    assert [timer[0] for timer in strategy.timers] == [True, False]


@pytest.mark.parametrize("runonce", [False, True])
def test_date_limit_flushes_only_the_requested_input(runonce):
    strategy, _ = run_delivery(tuple(range(1, 9)), runonce,
                               todate=datetime(2026, 1, 3))
    assert [event[0] for event in strategy.events] == [
        midnight(1), midnight(2), midnight(3), session_end(3)]
    assert strategy.events[-1][2] == (
        (3, midnight(3)), (2, midnight(3)), (2, session_end(3)))
    assert strategy.events[-1][1] == bt.num2date(session_end(3))


@pytest.mark.parametrize("runonce", [False, True])
def test_different_final_aggregate_times_are_delivered_in_order(runonce):
    dates = tuple(datetime(2026, 1, 1, 0, minute) for minute in range(1, 8))
    cerebro = bt.Cerebro(stdstats=False)
    cerebro.adddata(DailyFeed(days=dates, timeframe=bt.TimeFrame.Minutes))
    for compression in (2, 3):
        cerebro.resampledata(
            DailyFeed(days=dates, timeframe=bt.TimeFrame.Minutes),
            timeframe=bt.TimeFrame.Minutes, compression=compression)
    cerebro.addstrategy(RecordDelivery, timer_when=time(0, 8),
                         timer_repeat=timedelta(minutes=1))
    strategy, = cerebro.run(runonce=runonce)
    stamps = [bt.date2num(date) for date in dates]
    eighth = bt.date2num(datetime(2026, 1, 1, 0, 8))
    ninth = bt.date2num(datetime(2026, 1, 1, 0, 9))
    assert [event[0] for event in strategy.events] == stamps + [eighth, ninth]
    assert [event[1] for event in strategy.events] == list(map(
        bt.num2date, stamps + [eighth, ninth]))
    assert strategy.events[-2][2] == (
        (7, stamps[-1]), (4, eighth), (2, stamps[-2]))
    assert strategy.events[-1][2] == (
        (7, stamps[-1]), (4, eighth), (3, ninth))
    assert strategy.timers == [
        (cheat, datetime(2026, 1, 1, 0, minute),
         datetime(2026, 1, 1, 0, minute))
        for minute in (8, 9) for cheat in (True, False)
    ]


@pytest.mark.parametrize("runonce", [False, True])
def test_resampled_feed_without_original_delivers_partial_final_bar(runonce):
    cerebro = bt.Cerebro(stdstats=False)
    cerebro.resampledata(DailyFeed(days=(1, 2, 3)),
                         timeframe=bt.TimeFrame.Days, compression=2)
    cerebro.addstrategy(RecordDelivery)
    strategy, = cerebro.run(runonce=runonce)
    assert [event[0] for event in strategy.events] == [session_end(2), session_end(3)]
    assert [event[1] for event in strategy.events] == list(map(
        bt.num2date, (session_end(2), session_end(3))))


class TradeEachAggregate(RecordDelivery):
    def __init__(self):
        super().__init__()
        self.orders = []
        self.completed = []
        self._aggregate_count = 0

    def next(self):
        super().next()
        count = len(self.datas[-1])
        if count != self._aggregate_count:
            self._aggregate_count = count
            self.orders.append(self.buy(data=self.datas[0], size=1))

    prenext = next

    def notify_order(self, order):
        if order.status == order.Completed:
            self.completed.append((order.ref, order.executed.dt, order.executed.price))


@pytest.mark.parametrize("runonce", [False, True])
def test_final_delivery_does_not_repeat_strategy_orders(runonce):
    cerebro = bt.Cerebro(stdstats=False)
    days = tuple(range(1, 9))
    cerebro.adddata(DailyFeed(days=days))
    cerebro.resampledata(DailyFeed(days=days), timeframe=bt.TimeFrame.Days,
                         compression=2)
    cerebro.addstrategy(TradeEachAggregate)
    strategy, = cerebro.run(runonce=runonce)
    assert len(strategy.events) == 9
    assert len(strategy.orders) == 4
    assert len({order.ref for order in strategy.orders}) == 4
    assert [execution[1:] for execution in strategy.completed] == [
        (midnight(day), 100 + day) for day in (4, 6, 8)]
    assert strategy.orders[-1].status == bt.Order.Submitted


@pytest.mark.parametrize("runonce", [False, True])
def test_replay_feed_keeps_its_clock_alongside_final_resample(runonce):
    cerebro = bt.Cerebro(stdstats=False)
    days = (1, 2, 3, 4)
    cerebro.adddata(DailyFeed(days=days))
    cerebro.replaydata(DailyFeed(days=days), timeframe=bt.TimeFrame.Days,
                       compression=2)
    cerebro.resampledata(DailyFeed(days=days), timeframe=bt.TimeFrame.Days,
                         compression=2)
    cerebro.addstrategy(RecordDelivery)
    strategy, = cerebro.run(runonce=runonce)
    # Replay/resample must remain in next mode even when runonce is requested.
    assert not cerebro._dopreload
    assert [event[0] for event in strategy.events] == [
        midnight(1), midnight(2), midnight(3), midnight(4), session_end(4)]
    assert [event[2][1] for event in strategy.events] == [
        ((day + 1) // 2, midnight(day)) for day in days] + [(2, midnight(4))]
    assert strategy.events[-1][2][-1] == (2, session_end(4))
    assert strategy.events[-1][1] == bt.num2date(session_end(4))
