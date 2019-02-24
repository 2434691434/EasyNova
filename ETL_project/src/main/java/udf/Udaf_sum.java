package udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;

public class Udaf_sum extends AbstractGenericUDAFResolver {

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {

		if (info.length != 1) {
			throw new UDFArgumentException("INPUT MUST BU ONE LINE");
		}
		if (info[0].getCategory() != Category.PRIMITIVE) {
			throw new UDFArgumentException("CATEGORY MUST BU PRIMITIVE");
		}
		if (!info[0].getTypeName().equalsIgnoreCase(PrimitiveCategory.INT.name())) {
			throw new UDFArgumentException("TYPR MUST BE INT");
		}

		// TODO 自动生成的方法存根
		return new Sum_Evaluator();
	}

	public static class Sum_Evaluator extends GenericUDAFEvaluator {

		public static class SumAggrecationBuffer extends AbstractAggregationBuffer {
			private int sum = 0;

			public int getSum() {
				return sum;
			}

			public void setSum(int sum) {
				this.sum = sum;
			}

		}

		IntWritable tranferw = new IntWritable();
		IntWritable outputerw = new IntWritable();

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			System.out.println("Udaf_sum.Sum_Evaluator.getNewAggregationBuffer()");
			AggregationBuffer agg = new SumAggrecationBuffer();

			return agg;
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			System.out.println("Udaf_sum.Sum_Evaluator.reset()");
			SumAggrecationBuffer sumAgg = (SumAggrecationBuffer) agg;
			sumAgg.setSum(0);
		}

		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
			System.out.println("Udaf_sum.Sum_Evaluator.init()");
			super.init(m, parameters);

			return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			System.out.println("Udaf_sum.Sum_Evaluator.iterate()");
			SumAggrecationBuffer sumAgg = (SumAggrecationBuffer) agg;
			Object param = parameters[0];
			IntWritable inputw = null;
			if (param instanceof LazyInteger) {
				LazyInteger lz = (LazyInteger) param;
				inputw = lz.getWritableObject();
			} else {
				inputw = (IntWritable) param;
			}
			sumAgg.setSum(sumAgg.getSum() + inputw.get());
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			System.out.println("Udaf_sum.Sum_Evaluator.terminatePartial()");
			SumAggrecationBuffer sumAgg = (SumAggrecationBuffer) agg;
			tranferw.set(sumAgg.getSum());

			return tranferw;
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			System.out.println("Udaf_sum.Sum_Evaluator.merge()");
			SumAggrecationBuffer sumAgg = (SumAggrecationBuffer) agg;
			IntWritable inputw = null;
			if (partial instanceof LazyInteger) {
				LazyInteger lz = (LazyInteger) partial;
				inputw = lz.getWritableObject();
			} else {
				inputw = (IntWritable) partial;
			}
			sumAgg.setSum(sumAgg.getSum() + inputw.get());

		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			System.out.println("Udaf_sum.Sum_Evaluator.terminate()");
			SumAggrecationBuffer sumAgg = (SumAggrecationBuffer) agg;
			outputerw.set(sumAgg.getSum());

			return outputerw;
		}

	}

}
