import sys
from stocktick import StockTick
from pyspark import SparkContext

def maxValuesReduce(a, b):
    ### TODO: Return a StockTick object with the maximum value between a and b for each one of its
    ### four fields (price, bid, ask, units)
    return StockTick(price = max(a.price, b.price),
            bid = max(a.bid, b.bid),
            ask = max(a.ask, b.ask),
            units = max(a.units, b.units))

def minValuesReduce(a, b):
    ### TODO: Return a StockTick object with the minimum value between a and b for each one of its
    ### four fields (price, bid, ask, units)
    return StockTick(price = min(a.price, b.price),
            bid = min(a.bid, b.bid),
            ask = min(a.ask, b.ask),
            units = min(a.units, b.units))

def generateSpreadsDailyKeys(tick): ### TODO: Write Me (see below)
    dailyKey = '{2:0>4}-{0:0>2}-{1:0>2}'.format(*(tick.date.split('/')))
    return (dailyKey,
            (tick.time, (tick.ask - tick.bid) / 2 * (tick.ask + tick.bid), 1))

def generateSpreadsHourlyKeys(tick): ### TODO: Write Me (see below)
    hourlyKey = '{2:0>4}-{0:0>2}-{1:0>2}-{3:0>2}'.format(
            *(tick.date.split('/') + tick.time.split(':')))
    return (hourlyKey,
            (tick.time, (tick.ask - tick.bid) / 2 * (tick.ask + tick.bid), 1))

def spreadsSumReduce(a, b): ### TODO: Write Me (see below)
    if a[0] < b[0]:
        spreadsSum = a[1] + b[1]
        count = a[2] + b[2]
    else:
        spreadsSum = a[1]
        count = a[2]
    return (b[0], spreadsSum, count)

if __name__ == "__main__":
    """
    Usage: stock
    """
    sc = SparkContext(appName="StockTick")

    # rawTickData is a Resilient Distributed Dataset (RDD)
    # filename = "/opt/work/host/code/spark-exercises/WDC_tickbidask.txt"
    filename = "hdfs:///datasets/WDC_tickbidask.txt"
    rawTickData = sc.textFile(filename)

    tickData = rawTickData.map(lambda x: StockTick(x))
    goodTicks = tickData.filter(lambda x: x.price > 0.0 and x.bid > 0.0 and
            x.ask > 0.0 and x.units > 0)

    goodTicks.cache()
    numTicks = goodTicks.count()

    sumValues = goodTicks.reduce(lambda a,b: StockTick(price = a.price + b.price,
                                                    bid = a.bid + b.bid,
                                                    ask = a.ask + b.ask,
                                                    units = a.units + b.units))

    maxValuesReduce = goodTicks.reduce(maxValuesReduce) ### TODO: write the maxValuesReduce function
    minValuesReduce = goodTicks.reduce(minValuesReduce) ### TODO: write the minValuesReduce function
    avgUnits = sumValues.units / float(numTicks)
    avgPrice = sumValues.price / float(numTicks)

    print "Max units %i, avg units %f\n" % (maxValuesReduce.units, avgUnits)
    print "Max price %f, min price %f, avg price %f\n" \
        % (maxValuesReduce.price, minValuesReduce.price, avgPrice)

    ### Daily and hourly spreads
    # Here is how the daily spread is computed. For each data point, the spread can be calculated
    # using the following formula : (ask - bid) / 2 * (ask + bid)
    # 1) We have a MapReduce phase that uses the generateSpreadsDailyKeys() function as an argument
    #    to map(), and the spreadsSumReduce() function as an argument to reduce()
    #    - The keys will be a unique date in the ISO 8601 format (so that sorting dates
    #      alphabetically will sort them chronologically)
    #    - The values will be tuples that contain adequates values to (1) only take one value into
    #      account per second (which value is picked doesn't matter), (2) sum the spreads for the
    #    day, and (3) count the number of spread values that have been added.
    # 2) We have a Map phase that computes thee average spread using (b) and (c)
    # 3) A final Map phase formats the output by producing a string with the following format:
    #    "<key (date)>, <average_spread>"
    # 4) The output is written using .saveAsTextFile("WDC_daily")

    avgDailySpreads = goodTicks.map(generateSpreadsDailyKeys).reduceByKey(spreadsSumReduce); # (1)
    avgDailySpreads = avgDailySpreads.map(lambda a: (a[0], a[1][1] / a[1][2])) # (2)
    avgDailySpreads = avgDailySpreads.sortByKey().map(lambda a: '{0}, {1}'.format(*a)) # (3)
    avgDailySpreads = avgDailySpreads.saveAsTextFile("hdfs:///datasets/WDC_daily") # (4)

    # For the hourly spread you only need to change the key. How?

    avgHourlySpreads = goodTicks.map(generateSpreadsHourlyKeys).reduceByKey(spreadsSumReduce); # (1)
    avgHourlySpreads = avgHourlySpreads.map(lambda a: (a[0], a[1][1] / a[1][2])) # (2)
    avgHourlySpreads = avgHourlySpreads.sortByKey().map(lambda a: '{0}, {1}'.format(*a)) # (3)
    avgHourlySpreads = avgHourlySpreads.saveAsTextFile("hdfs:///datasets/WDC_hourly") # (4)

    sc.stop()
