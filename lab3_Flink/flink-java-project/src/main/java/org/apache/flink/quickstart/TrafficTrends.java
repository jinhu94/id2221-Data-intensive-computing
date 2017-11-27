package org.apache.flink.quickstart;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Map;
import java.util.Calendar;
import java.util.Iterator;
import java.util.TimeZone;
import org.joda.time.DateTime;
import scala.Int;

public class TrafficTrends {

    public enum JFKTerminal {
        TERMINAL_1(71436),
        TERMINAL_2(71688),
        TERMINAL_3(71191),
        TERMINAL_4(70945),
        TERMINAL_5(70190),
        TERMINAL_6(70686),
        NOT_A_TERMINAL(-1);

        int mapGrid;

        private JFKTerminal(int grid){
            this.mapGrid = grid;
        }

        public static JFKTerminal gridToTerminal(int grid){
            for(JFKTerminal terminal : values()){
                if(terminal.mapGrid == grid) return terminal;
            }
            return NOT_A_TERMINAL;
        }
    }
    public static int getFormattedTime(long timestamp) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("America/New_York"));
        calendar.setTimeInMillis(timestamp);
        return calendar.get(Calendar.HOUR_OF_DAY);
    }


    public static void main(String args[]) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // get the taxi ride data stream - Note: you got to change the path to your local data file
        DataStream<TaxiRide> terminalTraffic = env.addSource(
                new TaxiRideSource("nycTaxiRides.gz", 60, 2000));
        DataStream<Tuple3<JFKTerminal, Integer, Integer>> trends = terminalTraffic
                // map terminal numbers to events
                .map(new ridesTerminalMap())
                // filter rides start or end in terminals
                .filter(new RidesInTerminal())
                .map(new transferTime())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .apply(new RidesCount())
        /* until this line, the output is like this
        4> (TERMINAL_2,8,1)
        2> (TERMINAL_3,28,1)
        3> (TERMINAL_4,70,1)
        2> (TERMINAL_1,1,1)
        3> (TERMINAL_6,9,1)
        4> (TERMINAL_2,3,2)
        2> (TERMINAL_3,48,2)
        3> (TERMINAL_4,47,2)
        2> (TERMINAL_5,1,2)
        3> (TERMINAL_6,12,2)
        2> (TERMINAL_1,2,2)
        2> (TERMINAL_3,29,3)
        4> (TERMINAL_2,1,3)
        3> (TERMINAL_4,42,3)
        2> (TERMINAL_5,1,3)
        3> (TERMINAL_6,9,3)*/
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .apply(new MostPopularTerminal());
        /*output like this:
         (TERMINAL_4,30,19)
        2> (TERMINAL_3,39,20)
        3> (TERMINAL_3,16,21)
        4> (TERMINAL_3,3,22)
        1> (TERMINAL_4,14,23)
        2> (TERMINAL_4,45,0)
        3> (TERMINAL_4,70,1)
        4> (TERMINAL_3,48,2)
        1> (TERMINAL_4,42,3)
        2> (TERMINAL_4,45,4)
        3> (TERMINAL_4,31,5)
        4> (TERMINAL_3,26,6)
        1> (TERMINAL_4,50,7)
        2> (TERMINAL_3,57,8)
        3> (TERMINAL_3,73,9)
        4> (TERMINAL_3,52,10)
        1> (TERMINAL_3,55,11)
        2> (TERMINAL_3,47,12)
        3> (TERMINAL_3,58,13)*/

        trends.print();
        env.execute();

    }

    public static class ridesTerminalMap implements MapFunction<TaxiRide, Tuple2<JFKTerminal, DateTime>> {
        @Override
        public Tuple2<JFKTerminal, DateTime> map(TaxiRide taxiRide) throws Exception {
            int startGrid = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
            int endGrid = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
            JFKTerminal startTerminal = JFKTerminal.gridToTerminal(startGrid);
            JFKTerminal endTerminal = JFKTerminal.gridToTerminal(endGrid);
            if(taxiRide.isStart == true) {
                return new Tuple2<>(JFKTerminal.gridToTerminal(startGrid), taxiRide.startTime);
            }
            else {
                return new Tuple2<>(JFKTerminal.gridToTerminal(endGrid), taxiRide.startTime);
            }
        }
    }

    public static class RidesInTerminal implements FilterFunction<Tuple2<JFKTerminal, DateTime>> {
        @Override
        public boolean filter(Tuple2<JFKTerminal, DateTime> events) {
            return events.f0 != JFKTerminal.NOT_A_TERMINAL;
        }
    }

    public static class transferTime implements MapFunction<Tuple2<JFKTerminal, DateTime>, Tuple2<JFKTerminal, Integer>> {
        @Override
        public Tuple2<JFKTerminal, Integer> map(Tuple2<JFKTerminal, DateTime> formattedMap) throws Exception {
            int hour = getFormattedTime(formattedMap.f1.getMillis());
            //Calendar calendar = Calendar.getInstance();
            //calendar.setTimeZone(TimeZone.getTimeZone("America/New_York"));
            //calendar.setTimeInMillis(formattedMap.f1.getMillis());
            return new Tuple2<>(formattedMap.f0, hour);
        }
    }

    public static class RidesCount implements WindowFunction<Tuple2<JFKTerminal, Integer>,
                                                             Tuple3<JFKTerminal, Integer, Integer>,
                                                             Tuple,
                                                             TimeWindow> {
        @Override
        public void apply(Tuple key,
                          TimeWindow window,
                          Iterable<Tuple2<JFKTerminal, Integer>> values,
                          Collector<Tuple3<JFKTerminal, Integer, Integer>> out) throws Exception {
            JFKTerminal terminalID = key.getField(0);
            int hour = getFormattedTime(window.getStart());
            int count = 0;
            for(Tuple2<JFKTerminal, Integer> v: values) {
                count += 1;
            }
            out.collect(new Tuple3<>(terminalID, count, hour));
        }
    }
    public static class MostPopularTerminal implements AllWindowFunction<Tuple3<JFKTerminal, Integer, Integer>,
                                                                         Tuple3<JFKTerminal, Integer, Integer>,
                                                                         TimeWindow> {
        @Override
        public void apply(TimeWindow window,
                          Iterable<Tuple3<JFKTerminal, Integer, Integer>> values,
                          Collector<Tuple3<JFKTerminal, Integer, Integer>> out) {
            int hour = getFormattedTime(window.getStart());
            int max = 0;
            JFKTerminal CurrentTerminal = JFKTerminal.NOT_A_TERMINAL;
            for (Tuple3<JFKTerminal, Integer, Integer> v : values) {
                if (v.f1 > max) {
                    max = v.f1;
                    CurrentTerminal = v.f0;
                }
            }
            out.collect(new Tuple3<>(CurrentTerminal, max, hour));
        }
    }


}
