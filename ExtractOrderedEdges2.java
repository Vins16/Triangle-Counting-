package gebd.spark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

/*
 * classe utilizzata per ordinare i gli archi secondo il principio total order
 * dato (a,b) -> (a,b) t.c a<b
 */
public class ExtractOrderedEdges2 implements PairFlatMapFunction<String, String, String> {

	@Override
	public Iterator<Tuple2<String, String>> call(String arco) throws Exception {
		
		/* 
		 * creo una nuova lista destina ad ospitare le coppie <String, String>
		 * da restituire in output
		 */
		
		List<Tuple2<String, String>> output = new ArrayList<Tuple2<String, String>>();
		
		String[] nodi = arco.split(";");
		
		
				/*
				 * se una coppia è gia ordinata la lascio invariata
				 * altrimenti la riordino
				 */
				if(Integer.parseInt(nodi[0])<Integer.parseInt(nodi[1])) {
					output.add(new Tuple2<String, String>(nodi[0],nodi[1]));				
					}else {
						output.add(new Tuple2<String, String>(nodi[1],nodi[0]));
					}

		return output.iterator();
	}

}
