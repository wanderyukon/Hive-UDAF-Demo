import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

@Description(name = "maxrow", value = "_FUNC_(expr) - Returns the maximum value of expr and values of associated columns as a struct")
public class GenericUDAFMaxRow extends AbstractGenericUDAFResolver {

    /*创建LOG对象，用来写入警告和错误到 hive 的 log*/
    static final Log LOG = LogFactory.getLog(GenericUDAFMaxRow.class.getName());

    /* 验证数据类型，主要是实现操作符的重载。*/
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        // Verify that the first parameter supports comparisons.
        ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
        if (!ObjectInspectorUtils.compareSupported(oi)) {
            throw new UDFArgumentTypeException(0, "Cannot support comparison of map<> type or complex type containing map<>.");
        }
        return new GenericUDAFMaxRowEvaluator();
    }

    // @UDFType(distinctLike=true)
    public static class GenericUDAFMaxRowEvaluator extends GenericUDAFEvaluator {

        ObjectInspector[] inputOIs;
        ObjectInspector[] outputOIs;
        ObjectInspector structOI;

        /*
        *  实例化 Evaluator 类的时候调用的，在不同的阶段需要返回不同的 OI。
        *
        *
        *  嵌套类 Mode
        *
        *  PARTIAL1: 这个是 MapReduce 的 map 阶段: 从原始数据到部分数据聚合
        *            将会调用 iterate() 和 terminatePartial()
        *
        *  PARTIAL2: 这个是 MapReduce 的 map 端的 Combiner 阶段，负责在 map 端合并 map 的数据: 从部分数据聚合到部分数据聚合
        *            将会调用 merge() 和 terminatePartial()
        *
        *  FINAL:    MapReduce 的 reduce 阶段: 从部分数据的聚合到完全聚合
        *            将会调用 merge() 和 terminate()
        *
        *  COMPLETE: 如果出现了这个阶段，表示 MapReduce 只有 map，没有 reduce，所以 map 端就直接出结果了: 从原始数据直接到完全聚合
        *            将会调用 iterate() 和 terminate()
        *
        *  一般情况下，完整的UDAF逻辑是一个 mapreduce 过程，如果有 mapper 和 reducer，就会经历 PARTIAL1(mapper)，FINAL(reducer)，
        *  如果还有 combiner，那就会经历 PARTIAL1(mapper)，PARTIAL2(combiner)，FINAL(reducer)。
        *  而有一些情况下的 mapreduce，只有 mapper，而没有 reducer，所以就会只有 COMPLETE 阶段，这个阶段直接输入原始数据，出结果。
        * */
        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            System.out.println("init");
            super.init(mode, parameters);

            int length = parameters.length;
            if (length > 1 || !(parameters[0] instanceof StructObjectInspector)) {
                assert (mode == Mode.COMPLETE || mode == Mode.FINAL);
                initMapSide(parameters);

            } else {
                assert (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2);
                assert (parameters.length == 1 && parameters[0] instanceof StructObjectInspector);
                initReduceSide((StructObjectInspector) parameters[0]);
            }

            return structOI;
        }

        /* Initialize the UDAF on the map side. */
        private void initMapSide(ObjectInspector[] parameters) throws HiveException {
            System.out.println("initMapSide");
            int length = parameters.length;
            outputOIs = new ObjectInspector[length];
            List<String> fieldNames = new ArrayList<String>(length);
            List<ObjectInspector> fieldOIs = Arrays.asList(outputOIs);

            for (int i = 0; i < length; i++) {
                fieldNames.add("col" + i); // field names are not made available! :(
                outputOIs[i] = ObjectInspectorUtils.getStandardObjectInspector(parameters[i]);  // StandardObjectInspector
            }

            inputOIs = parameters;
            structOI = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);  // StructObjectInspector
        }

        /* Initialize the UDAF on the reduce side (or the map side in some cases). */
        private void initReduceSide(StructObjectInspector inputStructOI) throws HiveException {
            System.out.println("initReduceSide");
            List<? extends StructField> fields = inputStructOI.getAllStructFieldRefs();
            int length = fields.size();
            inputOIs = new ObjectInspector[length];
            outputOIs = new ObjectInspector[length];
            for (int i = 0; i < length; i++) {
                StructField field = fields.get(i);
                inputOIs[i] = field.getFieldObjectInspector();
                outputOIs[i] = ObjectInspectorUtils.getStandardObjectInspector(inputOIs[i]);
            }
            structOI = ObjectInspectorUtils.getStandardObjectInspector(inputStructOI);
        }

        static class MaxAgg implements AggregationBuffer {
            Object[] objects;
        }

        /* 获取存放中间结果的对象 */
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            System.out.println("getNewAggregationBuffer");
            MaxAgg result = new MaxAgg();
            return result;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            System.out.println("reset");
            MaxAgg maxagg = (MaxAgg) agg;
            maxagg.objects = null;
        }

        /*处理一行数据*/
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            System.out.println("iterate");
            merge(agg, parameters);
        }

        /* 返回部分聚合数据的持久化对象。
         * 因为调用这个方法时，说明已经是 map 或者 combine 的结束了，
         * 必须将数据持久化以后交给 reduce 进行处理。
         * 只支持JAVA原始数据类型及其封装类型、HADOOP Writable 类型、List、Map，
         * 不能返回自定义的类，即使实现了 Serializable 也不行，否则会出现问题或者错误的结果。*/
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            System.out.println("terminatePartial");
            return terminate(agg);
        }

        /*将 terminatePartial 返回的部分聚合数据进行合并，需要使用到对应的 OI。*/
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            System.out.println("merge");
            if (partial != null) {
                MaxAgg maxagg = (MaxAgg) agg;
                List<Object> objects;
                if (partial instanceof Object[]) {
                    objects = Arrays.asList((Object[]) partial);
                } else if (partial instanceof LazyBinaryStruct) {
                    objects = ((LazyBinaryStruct) partial).getFieldsAsList();
                } else {
                    throw new HiveException("Invalid type: " + partial.getClass().getName());
                }

                boolean isMax = false;
                if (maxagg.objects == null) {
                    isMax = true;
                } else {
                    int cmp = ObjectInspectorUtils.compare(maxagg.objects[0], outputOIs[0], objects.get(0), inputOIs[0]);
                    if (cmp < 0) {
                        isMax = true;
                    }
                }

                if (isMax) {
                    int length = objects.size();
                    maxagg.objects = new Object[length];
                    for (int i = 0; i < length; i++) {
                        maxagg.objects[i] = ObjectInspectorUtils.copyToStandardObject(objects.get(i), inputOIs[i]);
                    }
                }
            }
        }

        /*生成最终结果*/
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            System.out.println("terminate");
            MaxAgg maxagg = (MaxAgg) agg;
            return Arrays.asList(maxagg.objects);
        }
    }
}