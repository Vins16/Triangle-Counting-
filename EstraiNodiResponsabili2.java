package gebd.spark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;


/*
 * classe utilizzata per assegnare a ciascun nodo responsabile la coppia(A;B)
 * Dato (A;B,[(u1,u2,u3),$]) ->Restituisce (u1,A;B), (u2,A;B), (u3,A;B)
 */

public class EstraiNodiResponsabili2 implements PairFlatMapFunction<Tuple2<String, Tuple2<String, String>>, String, String> {

	@Override
	public Iterator<Tuple2<String, String>> call(Tuple2<String, Tuple2<String, String>> coppia) throws Exception {
		
		/* 
		 * creo una nuova lista destina ad ospitare le coppie <String, String>
		 * da restituire in output
		 */

		List<Tuple2<String, String>> output = new ArrayList<Tuple2<String, String>>();
		
		
		String[] nodiResponsabili = coppia._2._1.split(";");

		
		for ( String n: nodiResponsabili) {
        output.add(new Tuple2<String, String>(n, coppia._1));
		}

		
		return output.iterator();
		
	}

}
