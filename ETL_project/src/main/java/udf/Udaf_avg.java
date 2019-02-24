package udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;

public class Udaf_avg extends AbstractGenericUDAFResolver{

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
		
		if(info.length!=1){
			throw new UDFArgumentException("erro line must one");
			
		}
		if(info[0].getCategory()!=(Category.PRIMITIVE)){
			throw new UDFArgumentException("input must be primitive");
		}
		if(!info[0].getTypeName().equalsIgnoreCase(PrimitiveCategory.INT.name())){
			throw new UDFArgumentException("input type must be int");
		}
		
		return new AvgEvaluator();
	}
	
	public static class AvgEvaluator extends GenericUDAFEvaluator{

		public static class AvgAggregationBuffer extends AbstractAggregationBuffer{
			private int sum =0;
			private int count =0;
			public int getSum() {
				return sum;
			}
			public void setSum(int sum) {
				this.sum = sum;
			}
			public int getCount() {
				return count;
			}
			public void setCount(int count) {
				this.count = count;
			}
			
		}
		
		Object [] transferws = {new IntWritable() ,new IntWritable()};
		DoubleWritable outputw = new DoubleWritable();
		
		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			AggregationBuffer agg = new AvgAggregationBuffer();
			return agg;
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {

			AvgAggregationBuffer avgagg=(AvgAggregationBuffer)agg;
			avgagg.setCount(0);
			avgagg.setSum(0);
		}
		
		
		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
			super.init(m, parameters);
			
			if(m==m.PARTIAL1 || m==m.PARTIAL2){
				List<String> filenames = new ArrayList<String>();
				filenames.add("sum");
				filenames.add("count");
				
				List<ObjectInspector> inspector = new ArrayList<ObjectInspector>();
				inspector.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
				inspector.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
				
				return ObjectInspectorFactory.getStandardStructObjectInspector(filenames, inspector);
			}
			
			return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
		}
		
		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			
			AvgAggregationBuffer avgagg = (AvgAggregationBuffer)agg;
			Object param = parameters[0];
			IntWritable inputw = null;
			if(param instanceof LazyInteger){
				LazyInteger lz  = (LazyInteger)param;
				inputw = lz.getWritableObject();
			}else{
				inputw = (IntWritable)param;
			}
			
			avgagg.setSum(avgagg.getSum()+inputw.get());
			avgagg.setCount(avgagg.getCount()+1);
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			
			AvgAggregationBuffer avgagg = (AvgAggregationBuffer)agg;
			
			((IntWritable)transferws[0]).set(avgagg.getSum());
			((IntWritable)transferws[1]).set(avgagg.getCount());
			return transferws;
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			
			AvgAggregationBuffer avgagg = (AvgAggregationBuffer)agg;
			IntWritable inputsum = null;
			IntWritable inputcount = null;
			if(partial instanceof LazyBinaryStruct){
				LazyBinaryStruct lz = (LazyBinaryStruct)partial;
				inputsum = (IntWritable)lz.getField(0);
				inputcount = (IntWritable)lz.getField(1);
			}
			avgagg.setSum(avgagg.getSum()+inputsum.get());
			avgagg.setCount(avgagg.getCount()+inputcount.get());
			
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {

			AvgAggregationBuffer avgagg = (AvgAggregationBuffer)agg;
			double avg = (double)avgagg.getSum()/avgagg.getCount();
			outputw.set(avg);
			return outputw;
		}
		
	}
}
