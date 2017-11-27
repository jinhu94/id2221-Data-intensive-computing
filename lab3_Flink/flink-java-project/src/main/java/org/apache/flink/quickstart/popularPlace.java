package org.apache.flink.quickstart;

import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.basics.RideCleansing;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.scala.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.omg.PortableInterceptor.INACTIVE;



public class popularPlace {
    public static void main(String args[]) throws Exception {
        //set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final int popThreshold = 20;
        final int maxEventDelay = 60;
        final int servingSpeed = 600;
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //start the data generator
        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource("nycTaxiRides.gz", maxEventDelay, servingSpeed));

        //find popular places
        DataStream<Tuple5<Float, Float, Long, Boolean, Integer>> popularSpots = rides
                .filter(new RideCleansing.NYCFilter())
                .map(new GridCellMacher())
                .<KeyedStream<Tuple2<Integer, Boolean>, Tuple2<Integer, Boolean>>>keyBy(0, 1)
                .timeWindow(Time.minutes(15), Time.minutes(5))
                .apply(new RideCounter())
                .filter(new FilterFunction<Tuple4<Integer, Long, Boolean, Integer>>() {
                    @Override
                    public boolean filter(Tuple4<Integer, Long, Boolean, Integer> count) throws Exception {
                        return count.f3 > popThreshold;
                    }
                }).map(new GridCoordinates());
        popularSpots.print();
        env.execute("popular place:");


    }
    /**
     * Map taxi ride to grid cell and event type.
     * Start records use departure location, end record use arrival location.
     */
    public static class GridCellMacher implements MapFunction<TaxiRide, Tuple2<Integer, Boolean>> {
        @Override
        public Tuple2<Integer, Boolean> map(TaxiRide taxiRide) throws Exception{
            if(taxiRide.isStart){
                int gridId = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
                return new Tuple2<>(gridId, true);
            }
            else {
                int gridId = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
                return new Tuple2<>(gridId, false);
            }
        }
    }

    /**
     * Counts the number of rides arriving or departing.
     */
    public static class RideCounter implements WindowFunction<Tuple2<Integer, Boolean>, Tuple4<Integer, Long, Boolean, Integer>, Tuple, TimeWindow> {
        @SuppressWarnings("unchecked")
        @Override
        public void apply(
                Tuple key,
                TimeWindow window,
                Iterable<Tuple2<Integer, Boolean>> values,
                Collector<Tuple4<Integer, Long, Boolean, Integer>> out) throws Exception{

            int cellId = ((Tuple2<Integer, Boolean>)key).f0;
            boolean isStart = ((Tuple2<Integer, Boolean>)key).f1;
            long windowTime = window.getEnd();

            int cnt = 0;
            for (Tuple2<Integer, Boolean> v:values) {
                cnt += 1;
            }
            out.collect(new Tuple4<>(cellId, windowTime, isStart, cnt));
        }
    }


    /**
     * Maps the grid cell id back to longitude and latitude coordinates.
     */
    public static class GridCoordinates implements
            MapFunction<Tuple4<Integer, Long, Boolean, Integer>, Tuple5<Float, Float, Long, Boolean, Integer>> {
        @Override
        public Tuple5<Float, Float, Long, Boolean, Integer> map(
                Tuple4<Integer, Long, Boolean, Integer> cellCount) throws Exception {
            return new Tuple5<>(
                    GeoUtils.getGridCellCenterLon(cellCount.f0),
                    GeoUtils.getGridCellCenterLat(cellCount.f0),
                    cellCount.f1,
                    cellCount.f2,
                    cellCount.f3);
        }

    }

}
