package gebd.spark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;


/*
 * classe utilizzata per estrarre da ogni una tupla (Nodo, Vicini)
 * la stringa Vicini. In seguito per ogni stringa di vicini si estraggono
 * tutte le possibili coppie di nodi (senza ripetizioni)
 * restituendole nel formato (A;B, Nodoresponsabile)
 * 
 */
public class ExtractPairs2 implements PairFlatMapFunction<Tuple2<String,String>, String, String> {

	@Override
	public Iterator<Tuple2<String, String>> call(Tuple2<String,String> coppiaNodi) throws Exception {
		
		/* 
		 * creo una nuova lista destina ad ospitare le coppie <String, String>
		 * da restituire in output
		 */
		List<Tuple2<String, String>> output = new ArrayList<Tuple2<String, String>>();
		
		
		String[] nodi = coppiaNodi._2.split(";");
		for(String nodo1: nodi ) {
			for(String nodo2: nodi ) {
				/*
				 * per evitare di includere nell'output istanze del tipo:
				 * (A,A),1 oppure (A,B),1 e (B,A),1
				 * mi servo della relazione d'ordine sugli interi
				 * per escludere questi casi
				 * imponendo che la prima intero dei due
				 * sia inferiore all'altra
				 */
				if(Integer.parseInt(nodo1)<Integer.parseInt(nodo2)) {
					output.add(new Tuple2<String, String>(nodo1+";"+nodo2,coppiaNodi._1));
				}
				
					
			}
			
		}
		
		return output.iterator();
		
	}

}
