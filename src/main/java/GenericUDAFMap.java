import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;

import java.util.HashMap;
import java.util.Map;


@Description(name = "map_count", value = "_FUNC_(expr) - Return the map of column counting")
public class GenericUDAFMap extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(GenericUDAFMap.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        // 验证参数个数
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }
        // 验证参数类型
        switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
            case DOUBLE:
                return new GenericUDAFMapEvaluator();
            default:
                throw new UDFArgumentTypeException(0,
                        "Only numeric type arguments are accepted but "
                                + parameters[0].getTypeName() + " is passed.");
        }

    }

    public static class GenericUDAFMapEvaluator extends GenericUDAFEvaluator {

        private PrimitiveObjectInspector inputOI;
        private MapObjectInspector mapOI;

        /**
         * 实例化 Evaluator 类的时候调用的，在不同的阶段需要返回不同的 OI。
         * 嵌套类 Mode
         * PARTIAL1: 这个是 MapReduce 的 map 阶段: 从原始数据到部分数据聚合。将会调用 iterate() 和 terminatePartial()。
         * PARTIAL2: 这个是 MapReduce 的 map 端的 Combiner 阶段，负责在 map 端合并 map 的数据: 从部分数据聚合到部分数据聚合。将会调用merge() 和 terminatePartial()。
         * FINAL:    MapReduce 的 reduce 阶段: 从部分数据的聚合到完全聚合。将会调用 merge() 和 terminate()。
         * COMPLETE: 如果出现了这个阶段，表示 MapReduce 只有 map，没有 reduce，所以 map 端就直接出结果了: 从原始数据直接到完全聚合。将会调用 iterate() 和 terminate()。
         * 一般情况下，完整的UDAF逻辑是一个 mapreduce 过程，如果有 mapper 和 reducer，就会经历 PARTIAL1(mapper)，FINAL(reducer)，
         * 如果还有 combiner，那就会经历 PARTIAL1(mapper)，PARTIAL2(combiner)，FINAL(reducer)。
         * 而有一些情况下的 mapreduce，只有 mapper，而没有 reducer，所以就会只有 COMPLETE 阶段，这个阶段直接输入原始数据，出结果。
         *
         * @param m
         * @param parameters
         * @return
         * @throws HiveException
         */
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                assert (parameters[0] instanceof StandardMapObjectInspector);
                mapOI = (MapObjectInspector) parameters[0];
            }

            return ObjectInspectorFactory.getStandardMapObjectInspector(
                    PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
                    PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        }

        static class MapAgg implements AggregationBuffer {
            Map<Double, Integer> map = new HashMap<Double, Integer>();

            void add(Double o) {
                if (o == null) {
                    return;
                }
                if (map.containsKey(o)) {
                    map.put(o, map.get(o) + 1);
                } else {
                    map.put(o, 1);
                }
            }

            void add(Map<DoubleWritable, IntWritable> m) {
                for (Map.Entry<DoubleWritable, IntWritable> e : m.entrySet()) {
                    Double k = e.getKey().get();
                    if (map.containsKey(e.getKey())) {
                        map.put(k, map.get(k) + e.getValue().get());
                    } else {
                        map.put(k, e.getValue().get());
                    }
                }
            }
        }

        /**
         * 获取存放中间结果的对象
         *
         * @return
         * @throws HiveException
         */
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MapAgg ma = new MapAgg();
            return ma;
        }

        /**
         * @param agg
         * @throws HiveException
         */
        public void reset(AggregationBuffer agg) throws HiveException {
            ((MapAgg) agg).map.clear();
        }

        /**
         * 处理一行数据
         *
         * @param agg
         * @param parameters
         * @throws HiveException
         */
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            Double d = (Double) inputOI.getPrimitiveJavaObject(parameters[0]);
            MapAgg ma = (MapAgg) agg;
            ma.add(d);
        }

        /**
         * 返回部分聚合数据的持久化对象。
         * 因为调用这个方法时，说明已经是 map 或者 combine 的结束了，
         * 必须将数据持久化以后交给 reduce 进行处理。
         * 只支持JAVA原始数据类型及其封装类型、HADOOP Writable 类型、List、Map，
         * 不能返回自定义的类，即使实现了 Serializable 也不行，否则会出现问题或者错误的结果。
         *
         * @param agg
         * @return
         * @throws HiveException
         */
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        /**
         * 将 terminatePartial 返回的部分聚合数据进行合并，需要使用到对应的 OI。
         *
         * @param agg
         * @param partial
         * @throws HiveException
         */
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                MapAgg ma = (MapAgg) agg;
                ma.add((Map<DoubleWritable, IntWritable>) mapOI.getMap(partial));
            }
        }

        /**
         * 生成最终结果
         *
         * @param agg
         * @return
         * @throws HiveException
         */
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MapAgg ma = (MapAgg) agg;
            return ma.map;
        }
    }
}
