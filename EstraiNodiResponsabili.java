package gebd.spark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;


/*
 * classe utilizzata per assegnare a ciascuna stringa contente un nodoResponsabile
 * il valore 1
 * Dato A ->Restituisce (A,1)
 */
public class EstraiNodiResponsabili implements PairFlatMapFunction<String, String, Integer> {

	public Iterator<Tuple2<String, Integer>> call(String nodo) throws Exception {
		
		/* 
		 * creo una nuova lista destina ad ospitare le coppie <String, Integer>
		 * da restituire in output
		 */
		
		List<Tuple2<String, Integer>> output = new ArrayList<Tuple2<String, Integer>>();


	    output.add(new Tuple2<String, Integer>(nodo,1));
			

		
		return output.iterator();
	}



}
