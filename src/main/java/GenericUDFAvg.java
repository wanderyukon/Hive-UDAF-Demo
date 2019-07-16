import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import java.util.*;

@Description(
        name = "fp_avg",
        value = "_FUNC_(column) - Return the special average of fingerprint",
        extended = "select _FUNC_(column) from dual;")
public class GenericUDFAvg extends GenericUDF {

    static final Log LOG = LogFactory.getLog(GenericUDFAvg.class.getName());

    private MapObjectInspector mapOI;

    private class KV {
        private double k;
        private int v;

        public KV(double k, int v) {
            this.k = k;
            this.v = v;
        }

        public double getK() {
            return k;
        }

        public int getV() {
            return v;
        }
    }

    /**
     * 这个方法只调用一次，并且在evaluate()方法之前调用。
     * 该方法检查接受正确的参数类型和参数个数。
     *
     * @param arguments
     * @return
     * @throws UDFArgumentException
     */
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        if (arguments.length != 1) {
            throw new UDFArgumentException("The function fp_avg accepts 1 arguments.");
        }

        LOG.info(arguments[0].getClass().getName());
        if (!(arguments[0] instanceof MapObjectInspector)) {
            throw new UDFArgumentException("Only map type arguments are accepted but " + arguments[0].getTypeName() + " is passed.");
        }

        mapOI = (MapObjectInspector) arguments[0];

        if (!(mapOI.getMapKeyObjectInspector() instanceof DoubleObjectInspector) || !(mapOI.getMapValueObjectInspector() instanceof IntObjectInspector)) {
            throw new UDFArgumentException("The map must be type of <Double, Integer>");
        }

        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    /**
     * 这个方法类似UDF的evaluate()方法。它处理真实的参数，并返回最终结果。
     *
     * @param arguments
     * @return
     * @throws HiveException
     */
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        // 通过 ObjectInspector 获取参数的值
        Map<DoubleObjectInspector, IntObjectInspector> map = (HashMap<DoubleObjectInspector, IntObjectInspector>) mapOI.getMap(arguments[0].get());
        // 存放 map 的 list
        List<KV> kvl = new ArrayList<KV>(map.size());
        // 临时 键 值 变量
        double k;
        int v;
        // 值的总和
        int sum = 0;

        // 遍历 map 放入 list，并计算值的总和
        Iterator<Map.Entry<DoubleObjectInspector, IntObjectInspector>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<DoubleObjectInspector, IntObjectInspector> n = it.next();
            // ObjectInspector 可以强转为 Writable 类型 （org.apache.hadoop.io 包）
            k = ((DoubleWritable) n.getKey()).get();
            v = ((IntWritable) n.getValue()).get();
            kvl.add(new KV(k, v));
            sum += v;
        }

        // 将 list 根据 value 从大到小进行排序
        Collections.sort(kvl, new Comparator<KV>() {
            public int compare(KV o1, KV o2) {
                return o2.getV() - o1.getV();
            }
        });

        // 取 80% 的量
        int sum_8 = (int) (sum * 0.8);
        int last = sum_8;
        double result = 0;
        for (KV kv : kvl) {
            k = kv.getK();
            v = kv.getV();
            if (v <= last) {
                result += v * k;
                last -= v;
            } else {
                result += last * k;
                break;
            }
        }

        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector.create(result / sum_8);
    }

    /**
     * 这个方法用于当实现的GenericUDF出错的时候，打印出提示信息。
     * 而提示信息就是你实现该方法最后返回的字符串。
     *
     * @param children
     * @return error message
     */
    public String getDisplayString(String[] children) {
        return null;
    }
}
